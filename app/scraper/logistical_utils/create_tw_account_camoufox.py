# local
import numpy as np
from playwright.sync_api import Page, Locator, Cookie

from app.common.logger import get_logger, setup_logging
from app.scraper.hti.twitterActions_manual import TwitterActions
from app.scraper.hti.humanTwitterInteraction import process_cookies_out
from app.scraper.hti.scraping_utils.typer import Typer
from app.scraper.hti.scraping_utils.email_utils import get_gmail_verification_code, get_duck_duck_go_email
from app.scraper.my_utils.textverified_api import get_bearer_token, create_verification, get_verification_details, \
    get_correct_recent_sms_verification_code
from app.scraper.twscrape import Account
from app.scraper.twscrape.api import API

# libs
import asyncio
from httpx import AsyncClient
import random
from dotenv import load_dotenv
import json
import os.path
import time
import names

# typing
from typing import Callable, Optional

# Camoufox
from camoufox.sync_api import Camoufox

# playwright
from playwright.sync_api import Playwright

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


def attempt_to_click_multiple_times(locator: str, page: Page):
    attempts = 0
    while attempts < 5:
        try:
            pass
            page.locator(locator).click()
            time.sleep(1.5)
            return
        except Exception as e:
            logger.warning(f"Couldn't click on {locator} due to: {e}")
            attempts += 1
            time.sleep(2)
    pass


def start_login_flow() -> None:
    load_dotenv("../.env")
    asyncio.run(modify_exception_handler())

    from browserforge.fingerprints import Screen

    constrains = Screen(max_width=1920, max_height=1080)

    with Camoufox(humanize=True, screen=constrains) as browser:
        page = browser.new_page()
        page.goto("https://x.com/i/flow/signup")
        attempt_to_click_multiple_times("text=Create account", page)

        bearer = asyncio.run(get_bearer_token())
        phone_verification_href = asyncio.run(create_verification(bearer))
        phone_verification_creds = asyncio.run(
            get_verification_details(bearer_token=bearer, href=phone_verification_href))
        phone_number = phone_verification_creds["number"]

        name: Locator = page.locator('//input[@name="name"]')
        phone: Locator = page.locator('//input[@name="phone_number"]')
        dob: tuple[Locator, Locator, Locator] = page.locator('//select').all()
        (month, day, year) = dob

        name.type(names.get_full_name(), delay=abs(np.random.normal(0.8, 0.05)))
        time.sleep(1)
        phone.type(phone_number)
        time.sleep(1)
        month.select_option(months[random.randint(0, len(months) - 1)].title())
        time.sleep(1)
        day.select_option(str(random.randint(0, 28)))
        asyncio.sleep(2)
        year.select_option(str(random.randint(1980, 2005)))

        attempt_to_click_multiple_times("text=Next", page)

        # authenticate with Arkose
        input("Press ENTER after passing Arkose.")
        # verification code from phone
        time.sleep(5)
        phone_code = None
        for i in range(10):
            phone_verification_creds = asyncio.run(get_verification_details(bearer_token=bearer, href=phone_verification_href))

            # get SMS verification
            if "sms" in phone_verification_creds:
                phone_verification_sms_href = phone_verification_creds["sms"]['href']
                phone_verification_sms_creds = asyncio.run(get_verification_details(bearer_token=bearer,
                                                                              href=phone_verification_sms_href))
                correct_sms_verification = asyncio.run(get_correct_recent_sms_verification_code(phone_verification_sms_creds, seconds=180))
                if correct_sms_verification:
                    phone_code = correct_sms_verification
                    if phone_code:
                        break
            else:
                time.sleep(2)
        if not phone_code:
            raise Exception("No verification code found")
        else:
            verification_code_el = page.locator("text=Verification code")
            verification_code_el.type(str(phone_code), delay=abs(np.random.normal(0.3, 0.05)))
            time.sleep(5)

        attempt_to_click_multiple_times("text=Next", page)
        time.sleep(5)

        # password - from DuckDuckGo !
        time.sleep(5)
        password: str = get_duck_duck_go_email()
        email: str = f"{password}@duck.com"
        print(f"password: {password}", f"\nemail: {email}", f"\nphone number: {phone_number}")
        password_el = page.locator("text='Password'").first
        time.sleep(1)
        password_el.type(password, delay=abs(np.random.normal(0.3, 0.05)))
        time.sleep(5)

        sign_up_button = page.locator("text=Sign up").first
        sign_up_button.click()
        time.sleep(2)

        # pick a profile picture
        pic = asyncio.run(get_random_picture())
        pp_button = page.locator('//input[@data-testid="fileInput"]')
        pp_button.set_input_files(pic)
        try:
            apply = page.locator("text='Apply'").first
            apply.click()
            time.sleep(1)
            attempt_to_click_multiple_times("text=Next", page)
        except Exception as e:
            pass  # TODO: how get back to Skip for now if this fails ?
        time.sleep(1)

        # what should we call you ?
        username_el = page.locator('//input[name="username"]')
        username = username_el.get_attribute("value")

        # turn on notifications
        try:
            skip_for_now = page.locator("text=Skip for now").first
            skip_for_now.click()
            time.sleep(1)
        except:
            skip_for_now = page.locator('//button[data-testid="ocfEnterUsernameSkipButton"]')
            skip_for_now.click()
            time.sleep(1)

        try:
            skip_for_now = page.locator("text=Skip for now").first
            skip_for_now.click()
            time.sleep(1)
        except:
            skip_for_now = page.locator('//button[data-testid="ocfEnterUsernameSkipButton"]')
            skip_for_now.click()
            time.sleep(1)

        # What do you want to see on X? - general themes
        follow_candidates = page.locator('//button[aria-label^="Follow"]').all()
        indices = random.sample(range(len(follow_candidates)), 10)
        follow_candidates = [follow_candidates[i] for i in indices]
        num_follow = 0
        for f in follow_candidates:
            if num_follow >= 3:
                break
            try:
                f.scroll_into_view_if_needed()
                time.sleep(1)
                f.click()
                time.sleep(1)
                num_follow += 1
            except Exception as e:
                pass
        attempt_to_click_multiple_times("text=Next", page)
        time.sleep(3)

        # What do you want to see on X? - specific topics
        attempt_to_click_multiple_times("text=Next", page)
        time.sleep(3)

        # Don't miss out
        users = page.locator('//button[data-testid="UserCell"]').all()
        follow = page.locator("text=Follow").all()
        indices = random.sample(range(len(follow)), 15)
        follow = [follow[i] for i in indices]
        num_follow = 0
        for f in follow:
            if num_follow >= 3:
                break
            try:
                f.scroll_into_view_if_needed()
                time.sleep(2)
                f.click()
                time.sleep(2)
                num_follow += 1
            except Exception as e:
                pass
        attempt_to_click_multiple_times("text='Next'", page)
        time.sleep(5)

        # now just quit.
        quit_attempts = 0
        for i in range(5):
            quit_attempts += 1
            try:
                page.goto("https://x.com/home")
                time.sleep(2)
                page.locator('//div[aria-label="Timeline: Your Home Timeline"]')
                break
            except Exception as e:
                pass
        if quit_attempts == 4:
            input("Fix the issue.")

        # get to Account info
        # add email
        page.goto("https://x.com/i/flow/add_email")
        time.sleep(2)
        # potential password reqs
        try:
            password_input = page.locator("text=Password").first
            password_input.type(password, delay=abs(np.random.normal(0.3, 0.05)))
            time.sleep(2)
            attempt_to_click_multiple_times("text=Next", page)
        except:
            pass

        time.sleep(2)
        email_address_el = page.locator("text=Email address").first
        email_address_el.type(email, delay=abs(np.random.normal(0.3, 0.05)))
        time.sleep(2)
        attempt_to_click_multiple_times("text=Next", page)
        time.sleep(20)

        verification_code = asyncio.run(get_gmail_verification_code(email))
        verification_code_el = page.locator("text=Verification code").first
        verification_code_el.type(verification_code, delay=abs(np.random.normal(0.3, 0.05)))
        time.sleep(2)
        verify_el = page.locator("text=Verify").first
        verify_el.click()

        # Set automation
        api = API(
            pool="/Users/mikad/MEOMcGill/twitter_scraper/db/accounts.db"
        )
        acc = asyncio.run(api.pool.get_active())
        acc = [a for a in acc if (a.automated == 0) and ("duck" in a.email)]
        acc = acc[random.randint(0, len(acc) - 1)]
        automation_acc_username: str = acc.username
        automation_acc_password: str = acc.password
        page.goto("https://x.com/i/flow/enable_automated_account")
        time.sleep(2)

        phone_email_username = page.locator('//input[data-testid="ocfEnterTextTextInput"]')
        phone_email_username.type(automation_acc_username, delay=abs(np.random.normal(0.3, 0.05)))
        time.sleep(2)
        attempt_to_click_multiple_times("text=Next", page)
        time.sleep(2)
        password_el = page.locator("text=Password").first
        password_el.type(automation_acc_password, delay=abs(np.random.normal(0.3, 0.05)))
        time.sleep(2)
        login_button = page.locator("text=Log in").first
        login_button.click()
        time.sleep(2)

        # Delete number
        page.goto("https://x.com/settings/phone")
        time.sleep(2)
        delete_phone_number = page.locator("text=Delete phone number").first
        delete_phone_number.click()
        time.sleep(2)
        delete = page.locator("text=Delete").first
        delete.click()
        time.sleep(2)

        # Save user
        cookies: list[dict] = browser.contexts[0].cookies()
        cookies: dict = process_cookies_out(cookies)

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
        asyncio.run(api_new_account.pool.add_account(**account))

if __name__ == "__main__":
    start_login_flow()
