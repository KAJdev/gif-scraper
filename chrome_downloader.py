import asyncio

async def main():
    from pyppeteer import launch
    browser = await launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox'])
    page = await browser.newPage()
    await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
    print("Done! Chromium is downloaded.")