import asyncio
from camoufox.async_api import AsyncCamoufox
from browserforge.headers import Browser, HeaderGenerator
from browserforge.fingerprints import FingerprintGenerator
import requests

async def scraping_bla_bla():
    browsers: list = [Browser(name='firefox')]
    fingerprints = FingerprintGenerator(
        browser=browsers,
        locale="en",
        device='desktop',
    ).generate()

    # done
    resp = requests.get("https://www.google.com")
    await run_browser()
    pass
async def run_browser():
    async with AsyncCamoufox(headless=False) as browser:
        context = await browser.new_context(
            viewport={"width": 1024, "height": 768},
            user_agent=HeaderGenerator().generate()["User-Agent"]
        )
        page = await context.new_page()
        await page.goto("https://x.com/")
        await page.click('button[data-testid="xMigrationBottomBar"]')
        await page.click('a[data-testid="loginButton"]')
        pass
    pass
if __name__ == '__main__':
    #asyncio.run(scraping_bla_bla())
    asyncio.run(run_browser())