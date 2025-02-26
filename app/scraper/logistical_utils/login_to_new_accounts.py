import asyncio
import datetime
import json

import fake_useragent

from app.scraper.twscrape.api import API
from app.scraper.twscrape.account import Account
from app.scraper.hti.humanTwitterInteraction import humanize, HTIOutput


async def read_in_new_accounts(path_txt: str, size: int = 1):
    accounts = []
    api = API(pool="/Users/mikad/MEOMcGill/twitter_scraper/database/accounts.db")
    # open txt
    with open(path_txt, "r") as file:
        for r in file.readlines():
            parsed = r.strip().split(":")
            read_in_acc = {
                "username": parsed[0],
                "password": parsed[1],
                "email": parsed[2],
                "email_password": parsed[3],
                "token": parsed[4],
                "date_created": parsed[5],
                # "country": parsed[6].strip("country-")
            }
            processed_acc = {
                "username": read_in_acc["username"],
                "password": read_in_acc["password"],
                "email": read_in_acc["email"],
                "email_password": read_in_acc["email_password"],
                "twofa_id": read_in_acc["twofa_id"] if "twofa_id" in read_in_acc.keys() else None,
                'user_agent': fake_useragent.UserAgent(browsers="Chrome").chrome,
                'active': False
            }
            accounts_db_account = await api.pool.get_account(processed_acc["username"])
            if accounts_db_account:
                accounts.append(accounts_db_account)
            else:
                acc = Account(**processed_acc)
                accounts.append(acc)

    sem = asyncio.Semaphore(7)
    tasks = [humanize(a, size=size, headless=False, sem=sem) for a in accounts]
    results: tuple[HTIOutput] = await asyncio.gather(*tasks)
    for r in results:
        if r.login_status == 1:
            acc: Account = [a for a in accounts if a.username == r.username].pop()
            await api.pool.add_account(
                username=acc.username,
                password=acc.password,
                email=acc.email,
                email_password=acc.email_password,
                mfa_code=None,
                user_agent=acc.user_agent,
                cookies=json.dumps(r.cookies),
                use_case=4
            )
            await api.pool.save(acc)
            await api.pool.set_active(username=acc.username, active=True)
    pass


if __name__ == "__main__":
    asyncio.run(read_in_new_accounts("/Users/mikad/MEOMcGill/meo_twitter_scraper/twitter-crawler/temporary/twitter_accounts/10_2010_2015_confirmed_by_number_accounts.txt"))
