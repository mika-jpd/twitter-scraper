import asyncio
import datetime

from twscrape import account
from twscrape.api import API
from twscrape.account import Account
from hti.humanTwitterInteraction import humanize, HTIOutput


async def login_to_account(account: Account, sem: asyncio.Semaphore):
    async with sem:
        return await humanize(account, size=1, headless=True)


async def run():
    sem = asyncio.Semaphore(5)
    api = API(pool="/Users/mikad/MEOMcGill/twitter_scraper/accounts.db")
    accounts = await api.pool.get_all()
    accounts = [a for a in accounts if a.active == False]
    tasks = [login_to_account(a, sem) for a in accounts]
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
