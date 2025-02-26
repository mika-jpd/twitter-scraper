import asyncio
import datetime
import json

import requests

from app.scraper.twscrape.api import API
from app.scraper.twscrape.account import Account
from app.scraper.hti.humanTwitterInteraction import humanize, HTIOutput


async def login_to_account(account: Account, sem: asyncio.Semaphore, headless: bool = True):
    async with sem:
        return await humanize(account, size=1, headless=headless)

base_url = "http://localhost:8000"

async def run():
    sem = asyncio.Semaphore(6)
    headless = True
    response = requests.get(f"{base_url}/accounts",
                            params={"use_case": 0})
    accounts = response.json()["accounts"]
    accounts = [Account(**a) for a in accounts
                #if a["active"] == False
                ]
    print(f"Found {len(accounts)} inactive accounts from accounts.db")
    tasks = [login_to_account(a, sem, headless) for a in accounts]
    results: tuple[HTIOutput] = await asyncio.gather(*tasks)
    for res in results:
        acc: Account = [a for a in accounts if a.username == res.username].pop()
        acc.last_login = res.login_status
        acc.last_used = datetime.datetime.utcnow()
        acc.cookies = res.cookies if res.cookies else {}
        if res.login_status == 1:
            # set the account to active
            acc.active = True
        else:
            # set the account to inactive
            acc.active = False
            acc.error_msg = f"Login status {res.login_status} when running login_to_all_accounts locally."

        # save the account
        #await api.pool.save(acc)
        response = requests.post(f"{base_url}/accounts/save",
                                 json=json.loads(acc.json()))
        data = response.json()
        response.json()
    pass


if __name__ == "__main__":
    asyncio.run(run())
