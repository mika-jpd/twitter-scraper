from my_utils.logger.logger import logger
from playwright.async_api._generated import Page, Locator
from playwright._impl._errors import TimeoutError
from playwright.async_api import expect
from typing import Callable
from .scraping_utils.email import get_verification_code, get_verification_code_imap, get_authentication_code, \
    get_authentication_code_imap
import asyncio
import random
import httpx
import numpy as np
from typing import Awaitable, Any

AsyncCallable = Callable[[Any, Any], Awaitable[Any]]


def retry(max_tries: int = 3,
          sleep: float = 1,
          exceptions=(
                  TimeoutError
          ),
          retreat: float = 1.2,
          max_sleep: float = 5):
    def decorator(func):
        async def wrapper(*inner_args, **inner_kwargs):
            for i in range(max_tries):
                pause = min(sleep * retreat ** i, max_sleep)
                try:
                    return await func(*inner_args, **inner_kwargs)
                except exceptions as e:
                    logger.warning(
                        f"{g_username}: exception {e} raised for func {func} with *args {inner_args} **kwargs {inner_kwargs}")
                    await asyncio.sleep(pause)
            else:
                return False
        return wrapper
    return decorator


class TwitterActions:
    def __init__(self,
                 page: Page,
                 username: str):
        self.page: Page = page
        self.username: str = username
        global g_username
        g_username = self.username

    # login related methods
    @retry()
    async def login(self,
                    username: str,
                    password: str,
                    email: str,
                    email_password: str = None,
                    twofa_id: str = None
                    ) -> int:
        """
        Returns int with code:
            * 1 if login is successful
            * 0 if account is locked
            * -1 if the account has been suspended
            * -2 if the login failed for unknown reason
            * -3 too many login attempts
            * -4 suspicious login prevented
        """
        await self.page.goto("https://x.com")
        await asyncio.sleep(2)
        await self.check_got_it_message()

        # check if you've logged in
        if await self.check_login_success():
            await self.check_got_it_message()
            return 1
        else:
            try:
                logger.info(f"{username}: attempting manual login.")
                # manual login
                manual_login_success: int = await self.login_manually(username=username,
                                                                      password=password,
                                                                      email=email,
                                                                      email_password=email_password,
                                                                      twofa_id=twofa_id)
                if manual_login_success == 1:
                    await self.check_got_it_message()
                return manual_login_success
            except Exception as e:
                return -2

    async def login_manually(self,
                             username: str,
                             password: str,
                             email: str,
                             email_password: str,
                             twofa_id: str = None) -> int:
        await self.page.goto("https://x.com/i/flow/login")
        await self.expect_locator_visible_bool(self.page.locator('input[autocomplete="username"]'))
        await asyncio.sleep(random.uniform(10, 20) / 10)
        await self.page.locator('input[autocomplete="username"]').type(email, delay=100)
        await asyncio.sleep(random.uniform(10, 20) / 10)
        await self.page.get_by_text('Next').click()
        await asyncio.sleep(random.uniform(10, 20) / 10)

        # check if you need to put in email
        phone_or_username_button: bool = await self.expect_locator_visible_bool(
            self.page.get_by_text('Phone or username'), timeout=3000)
        if phone_or_username_button:
            await self.page.locator('input[data-testid="ocfEnterTextTextInput"]').type(username, delay=100)
            await asyncio.sleep(random.uniform(10, 20) / 10)
            await self.page.get_by_text('Next').click()
            await asyncio.sleep(random.uniform(10, 20) / 10)

        # check if you logged in too many times
        logged_in_too_many_times = await self.check_login_exceed_login_attempts()
        if logged_in_too_many_times:
            return -3

        # check if the accounts is locked
        account_locked = await self.check_login_account_locked()
        if account_locked:
            return 0

        # check if 'suspicious login prevented'
        suspicious_login_prevented = await self.check_login_prevented_suspicious_activity()
        if suspicious_login_prevented:
            return -3

        # continue with the password
        await self.page.locator('input[autocomplete="current-password"]').type(password, delay=100)
        await asyncio.sleep(random.uniform(10, 20) / 10)
        await self.page.locator('button[data-testid="LoginForm_Login_Button"]').click()
        await asyncio.sleep(random.uniform(10, 20) / 5)

        # potentially complete with 2fa
        two_fa_code = await self.expect_locator_visible_bool(self.page.get_by_text("Enter your verification code"),
                                                             timeout=3000)
        if two_fa_code:
            if twofa_id == None:
                logger.warning(f"{self.username}/: 2fa link required")
                return -2
            else:
                twofa_success: bool = await self.input_2fa_code(twofa_id)
                if not twofa_success:
                    return -2

        # potentially fetch confirmation email
        if await self.expect_locator_visible_bool(self.page.get_by_text("Check your email"), timeout=3000):
            await self.unlock_by_email_verification(email=email,
                                                    email_password=email_password)

        # then check if you can log in
        check_login = await self.check_login(username=username,
                                             password=password, email=email,
                                             email_password=email_password)
        return check_login

    async def input_2fa_code(self, twofa_id: str) -> bool:
        code = await self.fetch_2fa_code(twofa_id)
        if not code:
            logger.warning(f"{self.username}/: no 2fa code found")
            return False
        await self.page.locator('input[data-testid="ocfEnterTextTextInput"]').type(code, delay=100)
        for i in range(3):
            next_butn = self.page.get_by_text('Next')
            if await self.expect_locator_visible_bool(next_butn.first, timeout=2000):
                await next_butn.click()
                return True
            else:
                await asyncio.sleep(2)
        return False

    async def fetch_2fa_code(self, twofa_id) -> str | None:
        url = f"https://2fa.fb.rip/api/otp/{twofa_id}"
        otp: str | None = None
        restart_times: int = 0
        async with httpx.AsyncClient() as aclient:
            while True and restart_times < 5:
                # make some http requests, e.g.,
                try:
                    response = await aclient.get(url)
                except httpx.ConnectTimeout:
                    await asyncio.sleep(10)
                    restart_times += 1
                    continue
                result = response.json()

                if response.status_code == 200 and result["ok"] is True:
                    data = result["data"]
                    otp = data["otp"]
                    time_remaining = int(data["timeRemaining"])
                    if not (time_remaining > 15):
                        await asyncio.sleep(time_remaining)
                        restart_times += 1
                        continue
                    else:
                        return otp
                else:
                    return None

    async def check_login_suspended(self) -> bool:
        return await self.expect_locator_visible_bool(self.page.get_by_text(text="Your account is suspended"))

    async def check_login_success(self) -> bool:
        return await self.expect_locator_visible_bool(self.page.get_by_role(role="article").first)

    async def check_login_still_login_page(self) -> bool:
        return await self.expect_locator_visible_bool(self.page.get_by_test_id(test_id="loginButton"))

    async def check_login_exceed_login_attempts(self) -> bool:
        return await self.expect_locator_visible_bool(
            self.page.get_by_text(text="You have exceeded the number of allowed attempts"))

    async def check_login_prevented_suspicious_activity(self) -> bool:
        return await self.expect_locator_visible_bool(self.page.get_by_text("Suspicious login prevented"))

    async def check_login_account_locked(self) -> bool:
        return "x.com/account/access" in self.page.url

    async def start_account_unlock(self):
        start_dom = self.page.locator('input[type="submit"]')
        await start_dom.click()

    async def check_verification_email_required(self) -> bool:
        text = "Please verify your email address"
        dom = 'input[value="Send email"]'

        text_2 = "Check your email"
        dom_2 = 'input[data-testid="ocfEnterTextTextInput"]'

        text_3 = "We sent your verification code"
        dom_3 = 'input[placeholder="Enter Verification Code"]'

        check = await self.expect_locator_visible_bool(
            self.page.get_by_text(text)) and await self.expect_locator_visible_bool(self.page.locator(dom))
        if check:
            return True

        check = await self.expect_locator_visible_bool(
            self.page.get_by_text(text_2)) and await self.expect_locator_visible_bool(self.page.locator(dom_2))
        if check:
            return True

        check = await self.expect_locator_visible_bool(
            self.page.get_by_text(text_3)) and await self.expect_locator_visible_bool(self.page.locator(dom_3))
        if check:
            return True

    async def check_need_authentication(self) -> bool:
        return await self.expect_locator_visible_bool(self.page.locator('iframe[id="arkose_iframe"]'))

    async def unlock_by_email_verification(self,
                                           email: str,
                                           email_password: str):
        for _ in range(3):
            # click send email
            dom_send_email = self.page.locator('input[value="Send email"]')
            if await self.expect_locator_visible_bool(dom_send_email, timeout=2000):  # only need to do it sometimes
                await dom_send_email.click()

            email_type: str
            if await self.expect_locator_visible_bool(self.page.locator('input[placeholder="Enter Verification Code"]'),
                                                      timeout=1000):
                email_type = "verification"
            else:
                email_type = "authentication"

            if email_type == "verification":
                if "duck" in email:  # duck.com address gets forwarded to gmail account
                    code = await get_verification_code(
                        email_address=email,
                        service=None
                    )
                else:
                    code = await get_verification_code_imap(
                        email_address=email,
                        password=email_password
                    )
            elif email_type == "authentication":
                if "duck" in email:  # duck.com address gets forwarded to gmail account
                    code = await get_authentication_code(
                        email_address=email,
                        service=None
                    )
                else:
                    code = await get_authentication_code_imap(
                        email_address=email,
                        password=email_password
                    )
            else:  # should never happen
                raise TimeoutError("Invalid email type")

            if code:
                # first type of email verification
                if email_type == "verification":
                    dom = 'input[placeholder="Enter Verification Code"]'
                    verification_code_el = self.page.locator(dom)

                    await verification_code_el.fill(code)
                    await asyncio.sleep(1.5)

                    verify_button = self.page.locator('input[value="Verify"]')
                    await verify_button.click()
                    await asyncio.sleep(2)
                # second type of email verification
                elif email_type == "authentication":
                    dom = 'input[data-testid="ocfEnterTextTextInput"]'
                    verification_code_el = self.page.locator(dom)

                    await verification_code_el.fill(code)
                    await asyncio.sleep(1.5)

                    next_button = self.page.get_by_text("Next")
                    await next_button.click()
                    await asyncio.sleep(2)

                # check if you need to try again
                didnt_receive_email_button = self.page.locator('a[href="/account/access?lang=en&did_not_receive=true"]')
                if await self.expect_locator_visible_bool(didnt_receive_email_button, timeout=1000):
                    await didnt_receive_email_button.click()
                    await asyncio.sleep(2)
                else:
                    return True
            else:
                return False

    async def unlock_account(self,
                             email: str,
                             email_password: str,
                             email_required: bool = False) -> bool:
        if email_required:
            email_verified: bool = await self.unlock_by_email_verification(email=email, email_password=email_password)
            if not email_verified:
                return False

        authenticate_page = await self.check_need_authentication()
        if authenticate_page:
            return False

    async def check_login(self, username: str, password: str, email: str, email_password: str) -> int:

        """
        Returns int with code:
            * 1 if login is successful
            * 0 if account is locked
            * -1 if the account has been suspended
            * -2 if the login failed for unknown reason
            * -3 too many login attempts
        """
        # three cases: 1) permanently banned, 2) good, 3) some random error, 4) too many login attempts, 5) arkose challenge.
        # 1) permanently banned
        if await self.check_login_suspended():
            return -1
        # 2) managed to log in
        if await self.check_login_success():
            return 1

        # 3) still on login page
        if await self.check_login_still_login_page():
            return -2

        # 4) too many login attempts
        if await self.check_login_exceed_login_attempts():
            return -3

        # 5) suspicious activity
        if await self.check_login_prevented_suspicious_activity():
            return -3

        # 6) unlock your account
        if await self.check_login_account_locked():
            email_required: bool = await self.page.get_by_text("Verify your email address").is_visible()
            arkose_required: bool = await self.page.get_by_text("Pass an Arkose challenge").is_visible()
            # get on page saying your account has been locked
            if email_required or arkose_required:
                # click start
                await self.start_account_unlock()
            # don't get page
            else:
                email_required = await self.check_verification_email_required()
                # or check directly for the authentication
                arkose_required = await self.check_need_authentication()
            logger.warning(
                f"{self.username}: account locked with email {email_required} and Arkose {arkose_required}, "
                f"attempting unlock..."
            )
            unlocked: bool = await self.unlock_account(
                email_required=email_required,
                email=email,
                email_password=email_password
            )
            if unlocked:
                raise AttributeError(
                    "Rechecking whether your account is unlocked after successful unlock by triggering wrapper."
                )
            else:
                return 0

    @retry()
    async def get_homepage(self) -> Locator:
        await self.page.goto('https://x.com')
        await self.check_got_it_message()
        locator: Locator = self.page.locator(
            "div[aria-label='Timeline: Your Home Timeline'] div[data-testid='cellInnerDiv']").first
        await self.expect_locator_visible_bool(locator)
        return locator

    async def scroll(self, lim: int = None):
        vertical_scroll: int = 0
        if lim is None:
            num_scroll = np.random.randint(10, 20)
        else:
            num_scroll = lim
        for i in range(num_scroll):
            rand_scroll_amount = np.random.randint(250, 300)
            await self.page.mouse.wheel(delta_x=0, delta_y=rand_scroll_amount)
            vertical_scroll = vertical_scroll + rand_scroll_amount
            await asyncio.sleep(np.random.randint(10, 14) / 10)
        return vertical_scroll

    @retry()
    async def follow_back(self, name: str = "follow_back", progress: dict = None) -> bool:
        await self.check_got_it_message()
        if progress and name in progress.keys():
            progress[name]["attempts"] = progress[name]["attempts"] + 1

        await self.get_homepage()
        await self.page.goto(f"https://x.com/{self.username}/followers")

        # scroll
        # await self.page.mouse.wheel(delta_x=0, delta_y=200)
        await asyncio.sleep(random.randint(10, 22) / 10)

        # get followers
        not_following_list = self.page.locator(
            'div[aria-label="Timeline: Followers"] button[data-testid="UserCell"] button[aria-label^="Follow @"]')
        followed: int = 0
        for f in range(await not_following_list.count()):
            if followed >= 4:
                break
            locator = not_following_list.nth(f)
            # randomly follow with 30/70% chance
            p = random.randint(1, 10) >= 7
            if p:
                await locator.scroll_into_view_if_needed()
                await locator.click()
            followed += 1
            await asyncio.sleep(np.random.randint(10, 30) / 10)
        await asyncio.sleep(np.random.randint(10, 30) / 10)
        return True

    @retry()
    async def retweet(self, name: str = "retweet", progress: dict = None, num_retweets: int = 2) -> bool:
        await self.check_got_it_message()
        if progress and name in progress.keys():
            progress[name]["attempts"] = progress[name]["attempts"] + 1

        # get homepage
        await self.get_homepage()
        await asyncio.sleep(np.random.randint(10, 20) / 10)
        await self.scroll()

        # randomly find a retweet
        timeline_tweets_rt = self.page.locator(
            "div[aria-label='Timeline: Your Home Timeline'] div[data-testid='cellInnerDiv'] button[data-testid='retweet']")
        num_tweets_on_feed: int = await timeline_tweets_rt.count()
        if num_tweets_on_feed > 0:
            tweet_rt_buttons: list[int] = random.sample([i for i in range(num_tweets_on_feed)],
                                                        k=min(num_retweets, num_tweets_on_feed))
            tweet_rt_buttons: list[Locator] = [timeline_tweets_rt.nth(i) for i in tweet_rt_buttons]

            for t in tweet_rt_buttons:
                await t.scroll_into_view_if_needed()
                await t.click()

                # confirm retweet
                confirm_rt_dom = self.page.locator("div[data-testid='retweetConfirm']")
                await asyncio.sleep(np.random.randint(7, 12) / 10)
                await confirm_rt_dom.click()
                await asyncio.sleep(np.random.randint(15, 20) / 10)
            return True
        else:
            return False

    @retry()
    async def like(self, name="like", num_likes: int = 2, progress: dict = None) -> bool:
        await self.check_got_it_message()
        if progress and name in progress.keys():
            progress[name]["attempts"] = progress[name]["attempts"] + 1

        await self.get_homepage()
        await asyncio.sleep(np.random.randint(1, 20) / 10)
        await self.scroll()
        await asyncio.sleep(np.random.randint(15, 30) / 10)

        # randomly get a tweet & like
        timeline_tweets_like = self.page.locator(
            "div[aria-label='Timeline: Your Home Timeline'] div[data-testid='cellInnerDiv'] button[data-testid='like']")
        num_tweets_on_feed: int = await timeline_tweets_like.count()
        if num_tweets_on_feed > 0:
            tweet_like_buttons: list[int] = random.sample([i for i in range(num_tweets_on_feed)],
                                                          k=min(num_likes, num_tweets_on_feed))
            tweet_like_buttons: list[Locator] = [timeline_tweets_like.nth(i) for i in tweet_like_buttons]
            for t in tweet_like_buttons:
                await t.scroll_into_view_if_needed()
                await t.click()
                await asyncio.sleep(np.random.randint(10, 30) / 10)
            return True
        else:
            return False

    async def message(self) -> bool:
        return True
    @retry()
    async def view_trending(self, name: str = "view_trending", progress: dict = None) -> bool:
        await self.check_got_it_message()
        if progress and name in progress.keys():
            progress[name]["attempts"] = progress[name]["attempts"] + 1

        # 1. get homepage
        await self.get_homepage()
        await asyncio.sleep(np.random.randint(1, 20) / 10)

        # 2. navigate to explore (randomly for you and explore)
        # href="/explore/tabs/for_you" -> click to get to explore for you
        butn = self.page.locator('a[href="/explore/tabs/for_you"]')
        if await self.expect_locator_visible_bool(butn, timeout=2000):
            await butn.click()
        else:
            await self.page.goto("https://x.com/explore/tabs/for_you")
        await asyncio.sleep(np.random.randint(1, 20) / 20)

        # (50% chance of only scrolling down explore)
        if np.random.randint(0, 10) > 5:
            # href="/explore/tabs/keyword" -> click to get trending in Canada
            butn = self.page.locator('a[href="/explore/tabs/trending"]')
            if await self.expect_locator_visible_bool(butn, timeout=1500):
                await butn.click()
            else:
                await self.page.goto("https://x.com/explore/tabs/trending")
            await asyncio.sleep(np.random.randint(1, 20) / 20)

        # 3. scroll randomly
        await self.scroll()
        return True
    @retry()
    async def view_messages(self, name: str = "view_messages", progress: dict = None) -> bool:
        await self.check_got_it_message()
        if progress and name in progress.keys():
            progress[name]["attempts"] = progress[name]["attempts"] + 1

        await self.get_homepage()
        await asyncio.sleep(np.random.randint(10, 30) / 10)
        messages_dom: Locator = self.page.locator(selector='a[href="/messages"]')
        if await self.expect_locator_visible_bool(messages_dom):
            await messages_dom.click()
        else:
            await self.page.goto("https://x.com/messages")
        await asyncio.sleep(np.random.uniform(10, 30) / 10)

        await self.scroll(lim=2)

        return True
    @retry()
    async def view_homepage(self, name: str = "view_homepage", progress: dict = None) -> bool:
        await self.check_got_it_message()
        if progress and name in progress.keys():
            progress[name]["attempts"] = progress[name]["attempts"] + 1
        await self.get_homepage()
        await self.scroll(lim=7)
        return True

    async def check_got_it_message(self):
        got_it_butn = self.page.get_by_text("Got it")
        if await self.expect_locator_visible_bool(got_it_butn, timeout=2000):
            await got_it_butn.click()
        else:
            pass

    async def expect_locator_visible_bool(self, loc: Locator, timeout: int = 5000) -> bool:
        try:
            await expect(loc).to_be_visible(timeout=timeout)
            return True
        except AssertionError:
            return False
