import asyncio
import hrequests
from camoufox.async_api import AsyncCamoufox

async def scraping_bla_bla():
    resp = hrequests.get("https://www.google.com")
    run_browser()
    pass
async def run_browser():
    async with AsyncCamoufox(headless=False) as browser:
        page = await browser.new_page()
        await page.goto("https://x.com/")
        await page.click('button[data-testid="xMigrationBottomBar"]')
        await page.click('a[data-testid="loginButton"]')
        pass
    pass
if __name__ == '__main__':
    asyncio.run(run_browser())