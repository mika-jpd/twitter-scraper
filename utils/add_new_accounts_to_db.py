import asyncio
import datetime
import json

from twscrape import account
from twscrape.api import API
from twscrape.account import Account
from hti.humanTwitterInteraction import humanize, HTIOutput, HumanTwitterInteraction

from dataclasses import dataclass, asdict


@dataclass
class NewAccount:
    username: str
    password: str
    email: str
    email_password: str
    token: str
    location: str

    def dict(self) -> dict:
        return asdict(self)


def parse_account_from_txt_file(path: str) -> list[NewAccount]:
    accounts: list[NewAccount] = []
    with open(path, 'r') as f:
        lines = f.readlines()
        for l in lines:
            parsed_line = l.split(":")
            parsed_line = [i.strip() for i in parsed_line]
            acc = NewAccount(*parsed_line)
            accounts.append(acc)
    return accounts

async def run_humanization(sem: asyncio.Semaphore, acc: NewAccount) -> HTIOutput:
    async with sem:
        hti = HumanTwitterInteraction(
            username=acc.username,
            password=acc.password,
            email=acc.email,
            email_password=acc.email_password,
            headless=False
        )
        out = await hti.run_interaction(size=1)
    return out

async def main() -> None:
    sem = asyncio.Semaphore(1)
    accounts = parse_account_from_txt_file("/Users/mikad/MEOMcGill/meo_twitter_scraper/twitter-crawler/temporary/twitter_accounts/10_2010_2015_confirmed_by_number_accounts.txt")
    tasks = [run_humanization(sem, acc) for acc in accounts]
    output: tuple[HTIOutput] = await asyncio.gather(*tasks)

    # add / update some temporary database
    api = API(pool="/Users/mikad/MEOMcGill/twitter_scraper/new_account_db.db")
    for o in output:
        acc: NewAccount = [i for i in accounts if i.username == o.username].pop()

        await api.pool.add_account(
            username=acc.username,
            password=acc.password,
            email=acc.email,
            email_password=acc.email_password,
            cookies=json.dumps(o.cookies)
        )


if __name__ == "__main__":
    asyncio.run(main())