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
from app.common.models.accounts_models import NewTwitterAccountModel

logger = get_logger()


async def add_new_account(account: NewTwitterAccountModel, path_db: str, replace: bool = False) -> tuple[int, str]:
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

    # make API
    api = API(
        pool=path_db,
        _num_calls_before_humanization=(10000, 12000)
    )
    cookies = account.cookies
    cookies = process_cookies_in(cookies)
    if cookies is None:
        return 1, "Failed to process cookies; no ct0 or auth_token found in cookies !"

    cookies = [c.to_json() for c in cookies]
    cookies = {c["name"]: c["value"] for c in cookies}

    if replace:
        await api.pool.delete_accounts(usernames=account.username)
    else:
        presence = await api.pool.get_account(username=account.username)
        if presence:
            return 1, "Account already exists !"

    try:
        # add the account
        await api.pool.add_account(
            username=account.username,
            password=account.password,
            email=account.email,
            email_password=account.email_password,
            cookies=json.dumps(cookies),
            use_case=0,
            automated=account.automated
        )
        return 0, f"Successfully added account with username {account.username} !"
    except Exception as e:
        return 1, f"Failed to add account due to exception {e}"


async def humanize_account(username: str, path_db: str) -> tuple[int, str]:
    api = API(
        pool=path_db,
        _num_calls_before_humanization=(10000, 12000)
    )

    acc = await api.pool.get_account(username=username)
    r = await humanize(acc, headless=True)

    login_status = r.login_status
    u = r.username
    cookies = r.cookies

    return_code: int = 0
    msg: str = ""
    if login_status != 1:
        await api.pool.set_active(username=u, active=False,
                                  error_msg="Failed humanization in add_new_account.py/humanize_account.")
        return_code = 1
        msg = f"Failed to humanize account with username {username} with login_status {login_status}"
    else:
        await api.pool.set_active(username=u, active=True)
        await api.pool.set_cookies(username=u, cookies=cookies)
        msg = f"Successfully humanized account with username {username} with login_status {login_status}"

    return return_code, msg
