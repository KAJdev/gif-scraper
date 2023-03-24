import asyncio

async def main():
    from pyppeteer import launch
    browser = launch(executablePath='/usr/bin/google-chrome-stable')
    page = await browser.newPage()

if __name__ == "__main__":
    asyncio.run(main())
    print("Done! Chromium is downloaded.")