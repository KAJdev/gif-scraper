import asyncio
import datetime
import hashlib
import io
from os import getenv
import os
import random
from socket import gethostname
import aiohttp
from dataclasses import dataclass
import urllib

from pyppeteer import launch
import bs4
from tqdm import tqdm
import json
import base64
import motor.motor_asyncio
import asyncio
import aioboto3
import botocore
from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

scroll_pause_time = int(getenv('SCROLL_PAUSE_TIME', 0.5)) # You can set your own pause time. My laptop is a bit slow so I use 1 sec
no_more_iterations = int(getenv('NO_MORE_ITERATIONS', 5)) # Number of times to scroll down without finding new gifs before stopping
mal_pages = int(getenv('MAL_PAGES', 10)) # Number of pages to scrape from MyAnimeList
mal_pages_skip = int(getenv('MAL_PAGES_SKIP', 0)) # Number of pages to skip from MyAnimeList
gifs_per_anime = int(getenv('GIFS_PER_ANIME', 1000)) # Number of gifs to scrape per anime
binary_fetch_workers = int(getenv('BINARY_FETCH_WORKERS', 7)) # Number of workers to fetch binary data for gifs
browser_workers = int(getenv('BROWSER_WORKERS', 25)) # Number of workers to scrape gifs for anime
binary_chunk_size = int(getenv('BINARY_CHUNK_SIZE', 261120)) # Size of binary data to save in mongo
anime_batch_size = int(getenv('ANIME_BATCH_SIZE', 1)) # Number of anime to scrape per batch
sources = [
    "tenor",
    "alphacoders"
]

@dataclass(frozen=False, eq=True, order=True, repr=True, slots=True)
class TenorGif:
    src: str
    href: str
    search_term: str
    page: int
    index: int

    tags: list[str] = None
    data: bytes = None

    def __hash__(self) -> int:
        return hash(self.src)
    
    def __eq__(self, o: object) -> bool:
        if isinstance(o, TenorGif):
            return self.src == o.src
        return False
    
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
    
@dataclass(frozen=False, eq=True, order=True, repr=True, slots=True)
class AlphacodersGif:
    src: str
    page: int
    index: int

    tags: list[str] = None

    def __hash__(self) -> int:
        return hash(self.src)
    
    def __eq__(self, o: object) -> bool:
        if isinstance(o, AlphacodersGif):
            return self.src == o.src
        return False
    
    def to_dict(self):
        return {
            "src": self.src,
            "page": self.page,
            "index": self.index,
            "tags": self.tags
        }

def log(*msgs):
    # Concatenate the string "   " with the other messages
    message = "    " + " ".join(str(msg) for msg in msgs)
    # Pass the concatenated message to tqdm.write
    tqdm.write(message)

def log_imp(*msgs):
    # Concatenate the string "   " with the other messages
    message = "--> " + " ".join(str(msg) for msg in msgs)
    # Pass the concatenated message to tqdm.write
    tqdm.write(message)

async def scrape_search_gifs(browser, search_term, num_gifs):
    page = await browser.newPage()
    await page.goto("https://tenor.com/search/{}-gifs".format(search_term.replace(" ", "-")), {"timeout": 0})

    # wait for the page to load
    await page.waitForSelector('div.Gif img')

    log(f"{search_term} rendered: starting to scrape gifs")

    media = set()
    i = 0
    no_more_iters = 0

    pbar = tqdm(total=num_gifs, desc=f"Scraping {search_term}")

    while len(media) < num_gifs:
        images = await page.querySelectorAll('div.Gif img')
        before = len(media)

        for image in images:
            src = await page.evaluate('(element) => element.src', image)
            href = await page.evaluate('(element) => element.parentElement.parentElement.href', image)
            
            if href.startswith("https://tenor.com/gif-maker"):
                continue

            if (g := TenorGif(src, href, search_term, i, len(media))) not in media:
                pbar.update(1)
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

    pbar.close()

    log_imp(f"got {len(media)} gifs for {search_term}")

    await page.close()
    return media

async def get_top_mal_animes(pages=1):
    top = []
    skip = random.randint(0, 900)
    async with aiohttp.ClientSession() as session:
        for page in range(1, pages+1):
            async with session.get("https://api.jikan.moe/v4/top/anime?filter=bypopularity&page={}".format(page + skip)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for anime in data["data"]:
                        top.append(anime["title"] + " anime")

    return top

async def get_top_mal_characters(pages=1):
    """
    returns character names
    """
    top = []
    skip = random.randint(0, 1000)
    async with aiohttp.ClientSession() as session:
        for page in range(1, pages+1):
            async with session.get("https://api.jikan.moe/v4/top/characters?filter=bypopularity&page={}".format(page + skip)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for character in data["data"]:
                        top.append(character["name"] + " anime")

    return top

async def batch_workers(tasks, max_workers=10):
    results = []
    for i in range(0, len(tasks), max_workers):
        log_imp(f"running batch {i} to {i+max_workers}")
        batch = tasks[i:i+max_workers]
        results.extend(await asyncio.gather(*batch))
    return results

async def rolling_batch_workers(tasks, max_workers=binary_fetch_workers):
    results = []
    with tqdm(total=len(tasks)) as pbar:
        i = 0

        current_batch = tasks[:max_workers]
        tasks = tasks[max_workers:]

        while tasks or current_batch:
            if tasks and not current_batch:
                current_batch = tasks
                tasks = []

            done, unfinished = await asyncio.wait(current_batch, return_when=asyncio.FIRST_COMPLETED)
            pbar.update(len(done))
            i += len(done)

            for task in done:
                results.append(task.result())

            current_batch = list(unfinished)
            can_fill = max_workers - len(current_batch)
            if can_fill > 0:
                current_batch.extend(tasks[:can_fill])
                tasks = tasks[can_fill:]

    return results

async def fetch_gif_data(session, gif, s3, meta_col):
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

            if gif.data:

                s3_key = f"{gif.search_term.replace('/', '-')}/{gif.index}--{gif.src.split('/')[-1]}"

                # urlencode the key
                # s3_key = urllib.parse.quote(s3_key)

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
                    "file_s3_url": None,
                    "latent_s3_url": None,
                }

                # check if md5 already exists
                if await meta_col.find_one({"md5": meta["md5"]}):
                    del gif.data
                    gif.data = None
                    return False

                # upload to s3
                await s3.upload_fileobj(io.BytesIO(gif.data), getenv("SPACES_BUCKET"), s3_key, ExtraArgs={"ACL": "public-read", "ContentType": "image/gif"})

                meta["file_s3_url"] = f"https://{getenv('SPACES_BUCKET')}.sfo3.cdn.digitaloceanspaces.com/{s3_key}"
                # upload to db
                await meta_col.insert_one(meta)

                # free up memory
                del gif.data
                gif.data = None
                return True

    return False


async def alphacoders_worker(browser, session, s3, meta_col):
    fetched_total = 0
    uploaded_total = 0

    # for entropy
    await asyncio.sleep(random.random() * binary_fetch_workers * 2)

    while True:
        # fetch a page that hasn't been scraped yet
        log_imp("fetching new page number")
        result = await meta_col.aggregate([
            {"$match": {"source": "alphacoders"}},
            {"$group": {"_id": "$meta.page"}},
        ]).to_list(length=None)

        # the previous aggregation will return a list of dicts with the page number and the number of images on that page
        # we want to choose a page that is not in that list, under 3901, preferably not using random, just fill in the gaps
        # so we'll just loop through the list and find the first page that isn't in the list

        page_num = random.randint(1, 3900)
        for page_count in result:
            if page_count["_id"] != page_num:
                break
            page_num += 1

            if page_num > 3900:
                page_num = 0

        if page_num > 3901:
            log_imp("no more pages to scrape")
            break

        try:
            # new page
            page = await browser.newPage()
            await page.setViewport({"width": 1920, "height": 1080})
            await page.goto(f"https://gifs.alphacoders.com/by_category/3?page={page_num}", {"timeout": 0})

            # wait for the page to load
            await page.waitForSelector("div.thumb-container", {"timeout": 0})

            # get all text
            data = await page.content()

            # close the page
            await page.close()

            soup = bs4.BeautifulSoup(data, "html.parser")
            gifs = soup.find_all("div", {"class": "thumb-container"})

            i = 0

            for gif in tqdm(gifs, desc=f"    page {page_num}"):
                img = gif.find("img", {"class": "gif-thumb"})

                # image tags are located a couple divs down in a span with the class "thumb-categorization" as a list of a tags with the content as the tag
                tags = [tag.text for tag in gif.find("span", {"class": "thumb-categorization"}).find_all("a")]

                s3_key = f"alphacoders/{page_num}--{img['src'].split('/')[-1]}"

                # download gif
                data = await (await session.get(img["src"])).read()

                meta = {
                    "image_name": img["src"].split("/")[-1],
                    "meta": {
                        "view_url": img.parent["href"],
                        "src": img["src"],
                        "page": page_num,
                        "index": i,
                        "alphacoders_tags": tags,
                    },
                    "ext": "gif",
                    "source": "alphacoders",
                    "md5": hashlib.md5(data).hexdigest(),
                    "file_s3_url": None,
                    "latent_s3_url": None,
                }

                # check if md5 already exists
                if await meta_col.find_one({"md5": meta["md5"]}):
                    continue

                # upload to s3
                await s3.upload_fileobj(io.BytesIO(data), getenv("SPACES_BUCKET"), s3_key, ExtraArgs={"ACL": "public-read", "ContentType": "image/gif"})

                meta["file_s3_url"] = f"https://{getenv('SPACES_BUCKET')}.sfo3.cdn.digitaloceanspaces.com/{s3_key}"
                # upload to db
                await meta_col.insert_one(meta)

                i += 1
                uploaded_total += 1

            fetched_total += len(gifs)

        except Exception as e:
            log("ERROR:", e)

        log(f"uploaded {uploaded_total} images, fetched {fetched_total} images")

    return fetched_total, uploaded_total

    

async def run_workers(source: str):
    client = motor.motor_asyncio.AsyncIOMotorClient(getenv("MONGO_URI"), io_loop=asyncio.get_event_loop())
    db = client[getenv("MONGO_DB", 'animated_db_s3')]
    meta_col = db['animated_meta']
    s3_session = aioboto3.Session(
        aws_access_key_id=getenv("SPACES_KEY"),
        aws_secret_access_key=getenv("SPACES_SECRET"),
        region_name='sfo3'
    )

    fetched_total = 0
    uploaded_total = 0

    if source == "tenor":
        log_imp(f"fetching top anime and characters")
        all_terms = await get_top_mal_animes(mal_pages)
        log(f"fetched top anime:", all_terms)
        all_terms.extend(await get_top_mal_characters(mal_pages))
        log(f"fetched top anime and characters:", all_terms)

        # shuffle
        random.shuffle(all_terms)

        for anime_batch in range(0, len(all_terms), anime_batch_size):
            top_anime = all_terms[anime_batch:anime_batch+anime_batch_size]

            log_imp(f"fetching already fetched terms")
            before = len(top_anime)

            already_fetched_terms = await meta_col.aggregate([
                {"$group": {"_id": "$meta.search_term"}},
                {"$match": {"_id": {"$in": all_terms}}},
            ]).to_list(None)

            # flatten pipeline results to list
            already_fetched_terms = [term["_id"] for term in already_fetched_terms]

            # remove already fetched terms
            top_anime = [term for term in top_anime if term not in already_fetched_terms]

            log(f"removed {before - len(top_anime)} already fetched terms")

            if not top_anime:
                log("no anime to scrape")
                continue

            log_imp(f"launching browser")
            browser = await launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox'], executablePath='/usr/bin/google-chrome-stable' if os.name == 'posix' else None)
            log(f"browser launched")

            log_imp(f"scraping gifs")
            tasks = []
            for term in top_anime:
                tasks.append(scrape_search_gifs(browser, term, gifs_per_anime))
            log(f"created {len(tasks)} tasks")

            try:
                gifs = await batch_workers(tasks, browser_workers)
            except Exception as e:
                log("ERROR:", e)
            else:
                log("deduping and flattening gifs")
                gifs = [list(set(gif_set)) for gif_set in gifs]
                gifs = [gif for gif_set in gifs for gif in gif_set]
            finally:
                await browser.close()

            log(f"got {len(gifs)} gifs")

            log_imp(f"fetching gif metadata & saving binary data")

            async with s3_session.client('s3', endpoint_url=getenv("SPACES_ENDPOINT"), config=botocore.config.Config(s3={'addressing_style': 'virtual'})) as s3:
                async with aiohttp.ClientSession() as session:
                    tasks = [fetch_gif_data(session, gif, s3, meta_col) for gif in gifs]
                    results = await rolling_batch_workers(tasks, binary_fetch_workers)

                    for result in results:
                        if result:
                            uploaded_total += 1
                        fetched_total += 1

            log_imp(f"\n\nbatch {anime_batch+1} of {len(all_terms)}\n    fetched: {fetched_total} total\n    uploaded: {uploaded_total} total ({uploaded_total+1/fetched_total+1:.2%} success rate)")

    elif source == "alphacoders":
        try:
            log_imp(f"launching browser")
            browser = await launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox'], executablePath='/usr/bin/google-chrome-stable' if os.name == 'posix' else None)
            log(f"browser launched")

            log_imp(f"starting alphacoders workers")
            async with s3_session.client('s3', endpoint_url=getenv("SPACES_ENDPOINT"), config=botocore.config.Config(s3={'addressing_style': 'virtual'})) as s3:
                async with aiohttp.ClientSession() as session:
                    # spawn a bunch of workers to fetch gifs
                    tasks = [alphacoders_worker(browser, session, s3, meta_col) for _ in range(binary_fetch_workers)]
                    results = await asyncio.gather(*tasks)

                    for result in results:
                        if result:
                            fetched_total += result[0]
                            uploaded_total += result[1]
        finally:
            await browser.close()

def main(source: str | None = None):
    this_source = source or getenv("SOURCE", None)

    if not this_source:
        # seed with hostname
        log(f"seeding source choice with hostname: {gethostname()}")
        random.seed(gethostname())
        this_source = random.choice(sources)

    asyncio.run(run_workers(this_source))

if __name__ == "__main__":
    main()