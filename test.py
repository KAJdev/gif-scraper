import asyncio
import hashlib

import aiohttp


async def main():
    async with aiohttp.ClientSession() as session:
        async with session.get('https://media.tenor.com/XI6AinCEi-cAAAAC/nervous-zombielandsaga.gif') as resp:
            # md5
            print(hashlib.md5(await resp.read()).hexdigest())

if __name__ == '__main__':
    asyncio.run(main())