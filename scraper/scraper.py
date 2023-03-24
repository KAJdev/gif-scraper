import asyncio
import datetime
import hashlib
from os import getenv
import random
import aiohttp
from dataclasses import dataclass
from pymongo import ReturnDocument

from pyppeteer import launch
import bs4
from tqdm import tqdm
import json
import base64
import motor.motor_asyncio

from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

scroll_pause_time = int(getenv('SCROLL_PAUSE_TIME', 0.5)) # You can set your own pause time. My laptop is a bit slow so I use 1 sec
no_more_iterations = int(getenv('NO_MORE_ITERATIONS', 5)) # Number of times to scroll down without finding new gifs before stopping
mal_pages = int(getenv('MAL_PAGES', 1)) # Number of pages to scrape from MyAnimeList
mal_pages_skip = int(getenv('MAL_PAGES_SKIP', 0)) # Number of pages to skip from MyAnimeList
gifs_per_anime = int(getenv('GIFS_PER_ANIME', 10)) # Number of gifs to scrape per anime
binary_fetch_workers = int(getenv('BINARY_FETCH_WORKERS', 5)) # Number of workers to fetch binary data for gifs
browser_workers = int(getenv('BROWSER_WORKERS', 25)) # Number of workers to scrape gifs for anime
save_in_mongo = getenv('SAVE_IN_MONGO', "true").lower() == "true" # Whether to save gifs in mongo
binary_chunk_size = int(getenv('BINARY_CHUNK_SIZE', 261120)) # Size of binary data to save in mongo
anime_batch_size = int(getenv('ANIME_BATCH_SIZE', 1)) # Number of anime to scrape per batch

@dataclass(frozen=False, eq=True, order=True, repr=True, slots=True)
class GIF:
    src: str
    href: str
    search_term: str
    page: int
    index: int

    tags: list[str] = None
    data: bytes = None

    def __hash__(self) -> int:
        return hash(self.src)
    
    def to_dict(self):
        return {
            "src": self.src,
            "href": self.href,
            "search_term": self.anime,
            "page": self.page,
            "index": self.index,
            "tags": self.tags,
            "data": base64.b64encode(self.data).decode("utf-8") if self.data else None
        }

def log(*msgs):
    print("   ", *msgs)

def log_imp(*msgs):
    print("-->", *msgs)

async def scrape_search_gifs(browser, search_term, num_gifs):
    page = await browser.newPage()
    await page.goto("https://tenor.com/search/{}-gifs".format(search_term.replace(" ", "-")), {"timeout": 0})

    # wait for the page to load
    await page.waitForSelector('div.Gif img')

    log(f"{search_term} rendered: starting to scrape gifs")

    media = set()
    i = 0
    no_more_iters = 0

    while len(media) < num_gifs:
        images = await page.querySelectorAll('div.Gif img')
        before = len(media)

        for image in images:
            src = await page.evaluate('(element) => element.src', image)
            href = await page.evaluate('(element) => element.parentElement.parentElement.href', image)
            
            if href.startswith("https://tenor.com/gif-maker"):
                continue

            if (g := GIF(src, href, search_term, i, len(media))) not in media:
                media.add(g)

        if before >= len(media):
            no_more_iters += 1
            if no_more_iters >= no_more_iterations:
                log(f"no more gifs for {search_term}")
                break
        else:
            no_more_iters = 0
        
        # switch to page
        await page.bringToFront()

        await page.evaluate('window.scrollBy(0, window.innerHeight)')
        await asyncio.sleep(scroll_pause_time + random.random())
        i += 1

    log_imp(f"got {len(media)} gifs for {search_term}")

    await page.close()
    return media

async def get_top_mal_animes(pages=1):
    top = []
    async with aiohttp.ClientSession() as session:
        for page in range(1, pages+1):
            async with session.get("https://api.jikan.moe/v4/top/anime?filter=bypopularity?page={}".format(page + mal_pages_skip)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for anime in data["data"]:
                        top.append(anime["title"])                    

    return top

async def batch_workers(tasks, max_workers=10):
    results = []
    for i in range(0, len(tasks), max_workers):
        log_imp(f"running batch {i} to {i+max_workers}")
        batch = tasks[i:i+max_workers]
        results.extend(await asyncio.gather(*batch))
    return results

async def fetch_gif_data(session, gif):
    async with session.get(gif.href) as resp:
        if resp.status == 200:
            data = await resp.text()
            soup = bs4.BeautifulSoup(data, "html.parser")
            tags_meta = soup.find("meta", {"name": "keywords"})
            if tags_meta:
                gif.tags = tags_meta["content"].split(",")
            else:
                tqdm.write(f"    no tags for {gif.href}")

            src_meta = soup.find("meta", {"property": "og:image"})
            if src_meta:
                gif.data = await (await session.get(src_meta["content"])).read()
            else:
                tqdm.write(f"    no src for {gif.href}")

            return gif

async def run_workers():
    log_imp(f"fetching top anime")
    all_anime = await get_top_mal_animes(mal_pages)
    log_imp(f"fetched top anime:", all_anime)

    for anime_batch in range(0, len(all_anime), anime_batch_size):
        top_anime = all_anime[anime_batch:anime_batch+anime_batch_size]

        log_imp(f"launching browser")
        browser = await launch(headless=True, args=['--no-sandbox'])
        log(f"browser launched")

        log_imp(f"scraping gifs")
        tasks = []
        for term in top_anime:
            tasks.append(scrape_search_gifs(browser, term + " anime", gifs_per_anime))
        log(f"created {len(tasks)} tasks")

        try:
            gifs = await batch_workers(tasks, browser_workers)
        except Exception as e:
            print(e)
        else:
            log("deduping and flattening gifs")
            gifs = [list(set(gif_set)) for gif_set in gifs]
            gifs = [gif for gif_set in gifs for gif in gif_set]
        finally:
            await browser.close()

        log(f"got {len(gifs)} gifs")

        log_imp(f"fetching gif metadata & saving binary data")

        async with aiohttp.ClientSession() as session:
            with tqdm(total=len(gifs)) as pbar:
                # run in batches
                tasks = [fetch_gif_data(session, gif) for gif in gifs]
                i = 0

                current_batch = tasks[:binary_fetch_workers]
                tasks = tasks[binary_fetch_workers:]

                while tasks or current_batch:
                    if tasks and not current_batch:
                        current_batch = tasks
                        tasks = []

                    done, unfinished = await asyncio.wait(current_batch, return_when=asyncio.FIRST_COMPLETED)
                    pbar.update(len(done))
                    i += len(done)
                    current_batch = list(unfinished)
                    can_fill = binary_fetch_workers - len(current_batch)
                    if can_fill > 0:
                        current_batch.extend(tasks[:can_fill])
                        tasks = tasks[can_fill:]

        log(f"got {len(gifs)} gifs with metadata")

        if not save_in_mongo:
            jsonified = [gif.to_dict() for gif in gifs if gif.data]
            log_imp(f"writing {len(jsonified)} gifs to file")
            with open("gifs.txt", "w") as f:
                json_iter = json.JSONEncoder().iterencode(jsonified)
                for chunk in tqdm(json_iter, total=len(jsonified)):
                    f.write(chunk)
        else:
            log_imp(f"uploading {len(gifs)} gifs to mongo")

            client = motor.motor_asyncio.AsyncIOMotorClient(getenv("MONGO_URI"), io_loop=asyncio.get_event_loop())
            db = client['animated_db_test']

            chunks_col = db['animated_files.chunks']
            files_col = db['animated_files.files']
            meta_col = db['animated_meta']

            for gif in tqdm(gifs):
                if gif.data:
                    meta = {
                        "image_name": gif.src.split("/")[-1],
                        "meta": {
                            "tenor_tags": gif.tags,
                            "view_url": gif.href,
                            "src": gif.src,
                            "search_term": gif.search_term,
                            "page": gif.page,
                            "index": gif.index,
                        },
                        "ext": "gif",
                        "source": "tenor",
                        "md5": hashlib.md5(gif.data).hexdigest(),
                    }

                    file = {
                        "chunkSize": binary_chunk_size,
                        "length": len(gif.data),
                        "uploadDate": datetime.datetime.utcnow(),
                    }

                    file_id = await files_col.update_one(
                        {"md5": meta["md5"]},
                        {"$setOnInsert": file},
                        upsert=True,
                    )

                    if file_id.upserted_id is None:
                        tqdm.write(f"    skipping duplicate {meta['md5']}")
                        continue

                    chunks = []
                    for i in range(0, len(gif.data), binary_chunk_size):
                        chunks.append({
                            "data": gif.data[i:i+binary_chunk_size],
                            "n": i//binary_chunk_size,
                            "files_id": file_id.upserted_id,
                        })
                    await chunks_col.insert_many(chunks)

                    meta["_id"] = file_id.upserted_id
                    await meta_col.insert_one(meta)

        log_imp(f"done with batch {anime_batch} to {len(all_anime)}")

def main():
    asyncio.run(run_workers())

if __name__ == "__main__":
    main()