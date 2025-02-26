# local
import json
import os.path
import time

import names

from app.common.logger import get_logger, setup_logging
from app.scraper.hti.twitterActions_manual import TwitterActions
from app.scraper.hti.humanTwitterInteraction import process_cookies_out
from app.scraper.hti.scraping_utils.typer import Typer
from app.scraper.hti.scraping_utils.email_utils import get_gmail_verification_code, get_duck_duck_go_email
from app.scraper.my_utils.textverified_api import get_bearer_token, create_verification, get_verification_details, \
    get_correct_recent_sms_verification_code
from app.scraper.twscrape import Account
from app.scraper.twscrape.api import API

# modules
from asyncio.exceptions import TimeoutError
from zendriver import Tab, Browser, Element
from zendriver.core.connection import ProtocolException
import asyncio
import zendriver
from httpx import AsyncClient
import random
from dotenv import load_dotenv
import requests

# typing
from typing import Callable, Optional

setup_logging()
logger = get_logger()

months = [
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
]


async def check_element_presence(fn: Callable, **kwargs) -> Optional[Element | list[Element]]:
    try:
        el = await fn(**kwargs)
        return el
    except (TimeoutError, ProtocolException) as e:
        return None


async def find_and_click_next(tab: Tab):
    for i in range(5):
        el = await check_element_presence(tab.find, text="Next")
        if el:
            await el.click()
            return
        await asyncio.sleep(2)
    raise TimeoutError("The next button was not found !")


async def get_random_picture(
        dimension: tuple[int, int] = (1920, 1080)
) -> str:
    url = "https://picsum.photos"
    async with AsyncClient() as client:
        jpg = await client.get(url=url + f'/{dimension[0]}/{dimension[1]}?random',
                               follow_redirects=True)
        num = random.randint(0, 10000)
        path_random_images = os.path.join(f"/Users/mikad/MEOMcGill/twitter_scraper/app/scraper/my_utils/random_images",
                                          f"image_{num}.jpg")
        with open(path_random_images, "wb") as f:
            f.write(jpg.content)

        return path_random_images


async def modify_exception_handler():
    # define an exception handler
    def exception_handler(loop, context):
        # get details of the exception
        exception = context['exception']
        message = context['message']
        # log exception
        logger.error(f'Task failed, msg={message}, exception={exception}')

    # get the event loop
    loop = asyncio.get_running_loop()
    # set the exception handler
    loop.set_exception_handler(exception_handler)


async def start_login_flow(driver: Browser) -> None:
    load_dotenv("../.env")
    typer = Typer(accuracy=1, correction_chance=0.50, typing_delay=(0.1, 0.2), distance=2)
    # await self.typer.send(text=f"{email}", tab=self.tab, value='input[autocomplete="username"]')
    await modify_exception_handler()
    tab = await driver.get("https://x.com/i/flow/signup")
    create_account = await tab.find("Create account", best_match=True, timeout=10)
    await create_account.click()
    await asyncio.sleep(1)

    bearer = await get_bearer_token()
    phone_verification_href = await create_verification(bearer)
    phone_verification_creds = await get_verification_details(bearer_token=bearer, href=phone_verification_href)
    phone_number = phone_verification_creds["number"]

    # Create your account screen
    name = await tab.select(selector='input[name="name"]')
    phone = await tab.select(selector='input[name="phone_number"]')
    dob: list[Element] = await tab.select_all(selector='select')
    (month, day, year) = dob

    await typer.send(names.get_full_name(), tab=tab, value=name)
    await asyncio.sleep(1)
    await typer.send(phone_number, tab=tab, value=phone)
    await asyncio.sleep(1)
    await month.send_keys(months[random.randint(0, len(months) - 1)].title())
    await asyncio.sleep(2)
    await day.send_keys(str(random.randint(0, 28)))
    await asyncio.sleep(2)
    await year.send_keys(str(random.randint(1980, 2005)))
    await asyncio.sleep(2)
    await tab
    await find_and_click_next(tab)

    # authenticate with Arkose
    input("Press ENTER after passing Arkose.")
    # verification code from phone
    await asyncio.sleep(5)
    phone_code = None
    for i in range(10):
        phone_verification_creds = await get_verification_details(bearer_token=bearer, href=phone_verification_href)

        # get SMS verification
        if "sms" in phone_verification_creds:
            phone_verification_sms_href = phone_verification_creds["sms"]['href']
            phone_verification_sms_creds = await get_verification_details(bearer_token=bearer,
                                                                          href=phone_verification_sms_href)
            correct_sms_verification = await get_correct_recent_sms_verification_code(phone_verification_sms_creds,
                                                                                      seconds=180)
            if correct_sms_verification:
                phone_code = correct_sms_verification
                if phone_code:
                    break
        else:
            await asyncio.sleep(2)

    if not phone_code:
        raise Exception("No verification code found")
    else:
        verification_code_el = await tab.find("verification code", best_match=True, timeout=10)
        await verification_code_el.send_keys(str(phone_code))
        await asyncio.sleep(5)
    await find_and_click_next(tab)

    # password - from DuckDuckGo !
    await asyncio.sleep(5)
    password: str = get_duck_duck_go_email()
    email: str = f"{password}@duck.com"
    print(f"password: {password}", f"\nemail: {email}", f"\nphone number: {phone_number}")
    password_el = await tab.find("Password", best_match=True, timeout=10)
    await asyncio.sleep(1)
    await typer.send(password, tab=tab, value=password_el)
    await asyncio.sleep(5)
    sign_up_button = await tab.find("Sign up", best_match=True, timeout=10)
    await sign_up_button.click()
    await asyncio.sleep(2)
    # pick a profile picture
    pic = await get_random_picture()
    pp_button = await tab.select('input[data-testid="fileInput"]')
    await pp_button.send_file(pic)
    try:
        apply = await tab.find("Apply", best_match=True)
        await apply.click()
        await asyncio.sleep(1)
        await find_and_click_next(tab=tab)
    except Exception as e:
        pass  # TODO: how get back to Skip for now if this fails ?
    await asyncio.sleep(1)

    # what should we call you ?
    username_el = await tab.select('input[name="username"]')
    username = username_el.attrs["value"]

    # turn on notifications
    try:
        skip_for_now = await tab.find('Skip for now', best_match=True)
        await skip_for_now.click()
        await asyncio.sleep(1)
    except TimeoutError:
        skip_for_now = await tab.select('button[data-testid="ocfEnterUsernameSkipButton"]')
        await skip_for_now.click()
        await asyncio.sleep(1)

    try:
        skip_for_now = await tab.find('Skip for now', best_match=True)
        await skip_for_now.click()
        await asyncio.sleep(1)
    except TimeoutError:
        skip_for_now = await tab.select('button[data-testid="ocfEnterUsernameSkipButton"]')
        await skip_for_now.click()
        await asyncio.sleep(1)

    # What do you want to see on X? - general themes # TODO: can I just skip this? It seems that X creates the account before this happens !
    follow_candidates = await tab.select_all('button[aria-label^="Follow"]')
    follow_candidates = [follow_candidates[i] for i in random.sample(range(len(follow_candidates)), 10)]
    num_follow = 0
    for f in follow_candidates:
        if num_follow >= 3:
            break
        try:
            await f.scroll_into_view()
            await asyncio.sleep(1)
            await f.click()
            await asyncio.sleep(1)
            num_follow += 1
        except Exception as e:
            pass
    await find_and_click_next(tab)
    await asyncio.sleep(3)

    # What do you want to see on X? - specific topics
    await find_and_click_next(tab)
    await asyncio.sleep(3)

    # Don't miss out
    users = await tab.select_all('button[data-testid="UserCell"]')
    follow = await tab.find_all("Follow")
    follow = [follow[i] for i in random.sample(range(len(follow)), 15)]
    num_follow = 0  # TODO: it seems to capture "Follow" other than the follow buttons for suggested accounts
    for f in follow:
        if num_follow >= 3:
            break
        # follow = follow[random.randint(0, min(4, len(follow) - 1))]
        try:
            await f.scroll_into_view()
            await asyncio.sleep(2)
            await f.click()
            await asyncio.sleep(2)
            num_follow += 1
        except Exception as e:
            pass
    await find_and_click_next(tab)
    await asyncio.sleep(1)
    await asyncio.sleep(4)
    await tab

    # now just quit.
    quit_attempts = 0
    for i in range(5):
        quit_attempts += 1
        try:
            await tab.get("https://x.com/home")
            await asyncio.sleep(2)
            await tab.select('div[aria-label="Timeline: Your Home Timeline"]')
            break
        except Exception as e:
            pass
    if quit_attempts == 4:
        input("Fix the issue.")

    # get to Account info
    # add email
    await tab.get("https://x.com/i/flow/add_email")
    await asyncio.sleep(2)
    # potential password reqs
    password_input = await check_element_presence(tab.find, text="Password")
    await typer.send(password, tab=tab, value=password_input)
    await asyncio.sleep(2)
    await find_and_click_next(tab)

    await asyncio.sleep(2)
    email_address_el = await tab.find("Email address", best_match=True)
    await typer.send(email, tab=tab, value=email_address_el)
    await asyncio.sleep(2)
    await find_and_click_next(tab)
    await asyncio.sleep(20)

    verification_code = await get_gmail_verification_code(email)
    verification_code_el: Element = await tab.find("Verification code", best_match=True)
    await typer.send(verification_code, tab=tab, value=verification_code_el)
    await asyncio.sleep(2)
    verify_el = await tab.find("Verify", best_match=True)
    await verify_el.click()

    # Set automation
    api = API(
        pool="/Users/mikad/MEOMcGill/twitter_scraper/db/accounts.db"
    )
    acc: list[Account] = await api.pool.get_active()
    acc = [a for a in acc if (a.automated == 0) and ("duck" in a.email)]
    acc: Account = acc[random.randint(0, len(acc) - 1)]
    automation_acc_username: str = acc.username
    automation_acc_password: str = acc.password
    await tab.get("https://x.com/i/flow/enable_automated_account")
    await asyncio.sleep(2)
    await tab
    phone_email_username = await tab.select('input[data-testid="ocfEnterTextTextInput"]')
    await typer.send(automation_acc_username, tab=tab, value=phone_email_username)
    await asyncio.sleep(2)
    await find_and_click_next(tab)
    await asyncio.sleep(2)
    password_el: Element = await tab.find("Password", best_match=True)
    await typer.send(automation_acc_password, tab=tab, value=password_el)
    await asyncio.sleep(2)
    login_button = await tab.find("Log in", best_match=True)
    await login_button.click()
    await asyncio.sleep(2)
    # Delete number
    await tab.get("https://x.com/settings/phone")
    await asyncio.sleep(2)
    delete_phone_number = await tab.find("Delete phone number", best_match=True)
    await delete_phone_number.click()
    await asyncio.sleep(2)
    delete = await tab.find("Delete", best_match=True)
    await delete.click()
    await asyncio.sleep(2)

    cookies = await driver.cookies.get_all()

    # Scroll down the homepage
    tw_actions = TwitterActions(
        tab=tab,
        username=username,
        driver=driver
    )
    await tw_actions.view_homepage()

    # Save user
    cookies: list = await driver.cookies.get_all()
    cookies = [i.to_json() for i in cookies]
    cookies: dict = process_cookies_out(cookies)

    base_url = "http://localhost:8000"

    account = {
        "username": username,
        "password": password,
        "email": email,
        "email_password": "XXXXXXXXX",
        "use_case": 2,
        "cookies": json.dumps(cookies),
        "automated": True
    }

    api_new_account = API(
        pool="/Users/mikad/MEOMcGill/twitter_scraper/db/new_accounts.db"
    )
    await api_new_account.pool.add_account(
        **account
    )

    """response = requests.post(
        f"{base_url}/accounts/add_account",
        json=account
    )

    print(response.json())"""

    await driver.stop()


if __name__ == "__main__":
    # for i in range(5):
    driver = asyncio.run(
        zendriver.start(
            browser_executable_path="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            expert=True,
            headless=False,
            sandbox=False,
            browser_args=[
                # f'--user-agent={self.user_agent}',
                "--disable-gpu",
                "--disable-dev-shm-usage",
                "--disable-dev-tools",
                "--no-sandbox"
            ]
        )
    )
    asyncio.run(start_login_flow(driver=driver))
    # time.sleep(180)

"""
    try:
        skip_for_now = await tab.find('Skip for now', best_match=True)
        await skip_for_now.click()
    except TimeoutError:
        skip_for_now = await tab.select('button[data-testid="ocfEnterUsernameSkipButton"]')
        await skip_for_now.click()
        await asyncio.sleep(1)

    # turn on notifications
    try:
        skip_for_now = await tab.find('Skip for now', best_match=True)
        await skip_for_now.click()
        await asyncio.sleep(1)
    except TimeoutError:
        skip_for_now = await tab.select('button[data-testid="ocfEnterUsernameSkipButton"]')
        await skip_for_now.click()
        await asyncio.sleep(1)

    # What do you want to see on X? - general themes # TODO: can I just skip this? It seems that X creates the account before this happens !
    follow_candidates = await tab.select_all('button[aria-label^="Follow"]')
    follow_candidates = [follow_candidates[i] for i in random.sample(range(len(follow_candidates)), 10)]
    num_follow = 0
    for f in follow_candidates:
        if num_follow >= 3:
            break
        try:
            await f.scroll_into_view()
            await asyncio.sleep(1)
            await f.click()
            await asyncio.sleep(1)
        except Exception as e:
            pass
        num_follow += 1
    await find_and_click_next(tab)
    await asyncio.sleep(1)

    # What do you want to see on X? - specific topics
    await find_and_click_next(tab)
    await asyncio.sleep(1)

    # Don't miss out
    users = await tab.select_all('button[data-testid="UserCell"]')
    follow = await tab.find_all("Follow")
    follow = follow[random.randint(0, min(4, len(follow) - 1))]
    await follow.click()
    await asyncio.sleep(1)
    await find_and_click_next(tab)
    await asyncio.sleep(1)
    await asyncio.sleep(4)
    await tab
"""
