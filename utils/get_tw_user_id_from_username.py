import asyncio

from my_utils.meo_api.get_seeds import get_seeds
from my_utils.folder_manipulation.folder_manipulation import save_to_jsonl
from twscrape.api import API
from twscrape.models import User
import json
from dotenv import load_dotenv
import os
import tqdm.asyncio


async def save_user_info(user: User):
    with open(f"/Users/mikad/MEOMcGill/twitter_scraper/utils/twitter_users/{user.username}.json", "w+") as f:
        json.dump(user.dict(), f)
    pass


async def tw_username_to_id(tw_username: str, api: API, sem: asyncio.Semaphore) -> User:
    async with sem:
        user = await api.user_by_login(tw_username)
        if user:
            await save_user_info(user)
        return user


async def run():
    load_dotenv("/Users/mikad/MEOMcGill/twitter_scraper/.env")
    seeds = get_seeds(
        username=os.getenv('MEO_USERNAME'),
        password=os.getenv('MEO_PASSWORD')
    )
    sem = asyncio.Semaphore(15)
    sem_browsers = asyncio.Semaphore(5)
    api = API(pool="/Users/mikad/MEOMcGill/twitter_scraper/accounts.db",
              sem=sem_browsers,
              _num_calls_before_humanization=(30, 45))
    tasks = []

    # remove seeds you've already done
    finished_seeds = os.listdir("/Users/mikad/MEOMcGill/twitter_scraper/utils/twitter_users")
    finished_seeds = [s.replace(".json", "") for s in finished_seeds]
    seeds = [s for s in seeds if not s["Handle"] in finished_seeds]

    # now fetch the seeds
    for h in seeds:
        handle = h["Handle"]
        tasks.append(tw_username_to_id(tw_username=handle, api=api, sem=sem))
    active_accounts = await api.pool.get_all()
    active_accounts = [u for u in active_accounts if u.active is True]
    for u in active_accounts:
        username = u.username
        await api.pool.set_in_use(username=username, in_use=False)
    users = await asyncio.gather(*tasks)
    users = [u.dict() for u in users]
    pass

if __name__ == "__main__":
    asyncio.run(run())