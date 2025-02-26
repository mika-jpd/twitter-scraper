import asyncio
import json
import os
from typing import List, Optional
from dotenv import load_dotenv

from app.scraper.hti import HTIOutput
from app.scraper.twscrape import Account
from app.scraper.twscrape.api import API, User
from app.scraper.my_utils.meo_api import get_seeds
from app.common.logger import get_logger, setup_logging
from app.scraper.hti.humanTwitterInteraction import humanize, process_cookies_in

setup_logging()
logger = get_logger()
# fetch the handles
def get_handles(query: str, only_actives: bool = True) -> List[str]:
    seeds = get_seeds(query=query, only_actives=only_actives)
    return [s["Handle"] for s in seeds]


# get ID from handle
async def get_handle_twitter_id(
        handle: str,
        api: API,
        path: str,
        sem: asyncio.Semaphore):
    async with sem:
        user: User | None = await api.user_by_login(login=handle)
        if user:
            #logger.info(f'Got user {user.username} with display name {user.displayname}')
            with open(os.path.join(path, f"{handle}.json"), "w+") as f:
                f.write(json.dumps(user.dict()))
        else:
            logger.warning(f"Unable to find handle {handle}")


async def run():
    # define an exception handler
    def exception_handler(loop, context):
        # get details of the exception
        exception = context['exception']
        message = context['message']
        if not "sent 1000 (OK); then received 1000 (OK)" in str(exception):
            # log exception
            logger.error(f'Task failed, msg={message}, exception={exception}')

    # get the event loop
    loop = asyncio.get_running_loop()
    # set the exception handler
    loop.set_exception_handler(exception_handler)

    load_dotenv(dotenv_path="/Users/mikad/MEOMcGill/twitter_scraper/scraper/.env")

    # prepare list of handles
    handles = get_handles(query="Platform:Twitter", only_actives=True)
    path_handles = "/Users/mikad/MEOMcGill/twitter_scraper/scraper/my_utils/seed_manipulation/handle_to_user_information"
    scraped_handles = [i.replace(".json", "") for i in os.listdir(path_handles)]

    for i in os.listdir(path_handles):
        full_path = os.path.join(path_handles, i)
        tw_user_info = json.load(open(full_path, "r"))
        handle = i.replace(".json", "")
        if tw_user_info == {handle: "Not found"}:
            scraped_handles.remove(handle)

    handles = [handle for handle in handles if not handle in scraped_handles]

    sem_reqs = asyncio.Semaphore(20)
    sem_browser = asyncio.Semaphore(7)

    # make & run task
    api = API(
        pool="/Users/mikad/MEOMcGill/twitter_scraper/database/accounts.db",
        sem=sem_browser,
        _num_calls_before_humanization=(10000, 12000)
    )

    accs = await api.pool.get_all()
    for account in accs:
        await api.pool.set_in_use(account.username, False)
        await api.pool.set_active(account.username, True)

    # log into all accounts
    logger.info(f"Initial humanizing of {len(accs)} accounts !")
    humanize_tasks = [humanize(a, size=1, headless=True, sem=sem_browser) for a in accs]
    results: tuple[HTIOutput] = await asyncio.gather(*humanize_tasks)
    for r in results:
        login_status = r.login_status
        u = r.username
        cookies = r.cookies
        if login_status != 1:
            await api.pool.set_active(username=u, active=False, error_msg="Failed initial humanization.")
        else:
            await api.pool.set_active(username=u, active=True)
            await api.pool.set_cookies(username=u, cookies=cookies)
    accs = await api.pool.get_active()
    logger.info(f"After Humanizing there are {len(accs)} accounts !")

    # now run the task
    tasks = [get_handle_twitter_id(handle=h, api=api, path=path_handles, sem=sem_reqs) for h in handles]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except RuntimeError as e:
        logger.error(f"Error while running asyncio.run() due to error: {e}")