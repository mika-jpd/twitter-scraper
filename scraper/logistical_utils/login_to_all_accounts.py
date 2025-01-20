import asyncio
import datetime

from scraper.twscrape.api import API
from scraper.twscrape.account import Account
from scraper.hti.humanTwitterInteraction import humanize, HTIOutput


async def login_to_account(account: Account, sem: asyncio.Semaphore, headless: bool = True):
    async with sem:
        return await humanize(account, size=1, headless=headless)


async def run():
    sem = asyncio.Semaphore(5)
    headless = False
    api = API(pool="../accounts.db")
    accounts = await api.pool.get_all()
    accounts = [a for a in accounts if a.active == False]
    print(f"Found {len(accounts)} inactive accounts from accounts.db")
    tasks = [login_to_account(a, sem, headless) for a in accounts]
    results: tuple[HTIOutput] = await asyncio.gather(*tasks)
    for res in results:
        acc: Account = [a for a in accounts if a.username == res.username].pop()
        acc.last_login = res.login_status
        acc.last_used = datetime.datetime.utcnow()
        if res.login_status == 1:
            acc.active = True
            acc.cookies = res.cookies
        else:
            acc.active = False

        # save the account
        await api.pool.save(acc)
    pass


if __name__ == "__main__":
    asyncio.run(run())
