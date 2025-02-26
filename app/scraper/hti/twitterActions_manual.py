from typing import Callable
from app.scraper.hti.scraping_utils.email_utils import get_verification_code, get_verification_code_imap, get_authentication_code, get_authentication_code_imap
import asyncio
from asyncio.exceptions import CancelledError
import random
import httpx
import numpy as np
from typing import Awaitable, Any, Optional
# from nodriver import Tab, Element
import zendriver
from zendriver import Tab, Element, Browser
from app.scraper.hti.scraping_utils.typer import Typer
from app.common.logger import get_logger
from websockets.exceptions import ConnectionClosedOK, ConnectionClosed

AsyncCallable = Callable[[Any, Any], Awaitable[Any]]
logger = get_logger()

def retry(max_tries: int = 3,
          sleep: float = 1,
          exceptions=(
                  TimeoutError,
                  ConnectionClosedOK,
                  ConnectionClosed,
                  CancelledError,
                  AttributeError,
                  zendriver.core.connection.ProtocolException
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
                 tab: Tab,
                 username: str,
                 driver: Browser,
                 typer: Optional[Typer] = None
                 ):
        self.tab: Tab = tab
        self.username: str = username
        self.driver: Browser = driver
        global g_username
        g_username = self.username
        if typer:
            self.typer = typer
        else:
            self.typer = Typer(accuracy=1, correction_chance=0.50, typing_delay=(0.1, 0.2), distance=2)

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
        await self.tab.get("https://x.com")
        await asyncio.sleep(2)
        await self.check_got_it_message()
        await self.check_youre_in_control()

        # check if you've logged in
        if await self.check_login_success():
            await self.check_got_it_message()
            await self.check_youre_in_control()
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
                    await self.check_youre_in_control()
                return manual_login_success
            except Exception as e:
                logger.warning(f"{username} - exception occurred while logging in manually: {e.__class__}: {e}")
                return -2

    # login-related methods
    @retry(max_tries=2)
    async def login_manually(self,
                             username: str,
                             password: str,
                             email: str,
                             email_password: str,
                             twofa_id: str = None) -> int:
        await self.driver.cookies.clear()
        await asyncio.sleep(3)
        await self.tab.get("https://x.com/i/flow/login")
        await asyncio.sleep(2)  # TODO: it seems that this is where you need to give it a second for DOM elements to load
        await self.tab.wait_for(selector='input[autocomplete="username"]')
        await self.tab.select('input[autocomplete="username"]')
        await asyncio.sleep(random.uniform(10, 20) / 10)
        await self.typer.send(text=f"{email}", tab=self.tab, value='input[autocomplete="username"]')
        await asyncio.sleep(random.uniform(10, 20) / 10)
        next_button = await self.tab.find('Next', best_match=True)
        await next_button.click()
        await asyncio.sleep(random.uniform(10, 20) / 10)

        # check if you need to put in email
        try:
            phone_or_username_button = await self.tab.find('Phone or username', best_match=True, timeout=3)
        except TimeoutError:
            phone_or_username_button = None
        if phone_or_username_button:
            await self.typer.send(text=self.username, tab=self.tab, value='input[data-testid="ocfEnterTextTextInput"]')
            await asyncio.sleep(random.uniform(10, 20) / 10)
            next_button = await self.tab.find('Next')
            await next_button.click()
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
        await self.tab.select('input[autocomplete="current-password"]')
        await self.typer.send(text=password, tab=self.tab, value='input[autocomplete="current-password"]')
        await asyncio.sleep(random.uniform(10, 20) / 10)
        login_button = await self.tab.select('button[data-testid="LoginForm_Login_Button"]')
        await login_button.click()
        await asyncio.sleep(random.uniform(10, 20) / 5)
        # Review your phone - message presence; Yes, that is my number - button; click - button; Done
        # potentially complete with 2fa
        try:
            two_fa_code = await self.tab.find("Enter your verification code", timeout=3)
        except TimeoutError:
            two_fa_code = None
        if two_fa_code:
            await self.input_2fa_code(
                username=username,
                password=password, twofa_id=twofa_id
            )

        # potentially fetch confirmation email
        try:
            verification_code = await self.tab.find("Check your email", timeout=3)
        except TimeoutError:
            verification_code = None
        if verification_code:
            await self.unlock_by_email_verification(email=email, email_password=email_password)

        # then check if you can log in
        check_login = await self.check_login(username=username,
                                             password=password,
                                             email=email,
                                             email_password=email_password)
        return check_login

    async def check_login_success(self) -> bool:
        try:
            await self.tab.select("div[aria-label='Timeline: Your Home Timeline'] div[data-testid='cellInnerDiv']",
                                  timeout=3)
            return True
        except TimeoutError:
            return False

    async def check_login_exceed_login_attempts(self) -> bool:
        try:
            await self.tab.find("You have exceeded the number of allowed attempts", timeout=2, best_match=True)
            return True
        except TimeoutError:
            return False

    async def check_login_account_locked(self) -> bool:
        account_locked_page: bool = ("x.com/account/access" in self.tab.target.url
                                     or
                                     await self.check_your_account_locked())
        return account_locked_page

    async def check_your_account_locked(self, timeout: int = 3) -> bool:
        try:
            await self.tab.find('Your account has been locked', timeout=timeout)
            return True
        except TimeoutError:
            return False

    async def check_login_prevented_suspicious_activity(self) -> bool:
        try:
            await self.tab.find("Suspicious login prevented", timeout=2, best_match=True)
            return True
        except TimeoutError:
            return False

    async def input_2fa_code(self, username: str, password: str, twofa_id):
        if not twofa_id:
            logger.warning(f"{username}/{password}: 2fa link required")
            return -2
        code = await self.fetch_2fa_code(twofa_id=twofa_id)
        if not code:
            logger.warning(f"{username}/{password}: no 2fa code found")
            return -2
        await self.typer.send(text=code, tab=self.tab, value='input[data-testid="ocfEnterTextTextInput"]')
        for i in range(3):
            next_button = await self.tab.find('Next')
            if next_button:
                await next_button.click()
                break
            else:
                await asyncio.sleep(2)

    async def fetch_2fa_code(self, twofa_id) -> str | None:
        url = f"https://2fa.fb.rip/api/otp/{twofa_id}"
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

    @retry(max_tries=1)
    async def unlock_by_email_verification(self, email: str, email_password: str) -> bool:
        for _ in range(3):
            try:
                # click send email
                dom_send_email = await self.tab.select('input[value="Send email"]')
                await dom_send_email.click()
                await asyncio.sleep(2)
            except asyncio.TimeoutError:
                # sometimes you don't need to click !
                pass
            email_type: str
            try:
                dom = 'input[placeholder="Enter Verification Code"]'
                await self.tab.select(dom)
                email_type = "verification"
            except asyncio.TimeoutError:
                dom = 'input[data-testid="ocfEnterTextTextInput"]'
                await self.tab.select(dom)
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
                    verification_code_el = await self.tab.select(dom)

                    await verification_code_el.send_keys(code)
                    await asyncio.sleep(1.5)

                    verify_button = await self.tab.select('input[value="Verify"]')
                    await verify_button.click()
                    await asyncio.sleep(2)
                # second type of email verification
                elif email_type == "authentication":
                    dom = 'input[data-testid="ocfEnterTextTextInput"]'
                    verification_code_el = await self.tab.select(dom)

                    await verification_code_el.send_keys(code)
                    await asyncio.sleep(1.5)

                    next_button = await self.tab.find("Next", best_match=True)
                    await next_button.click()
                    await asyncio.sleep(2)

                # check if you need to try again
                try:
                    await self.tab.select('span[class="Form-message is-errored"]')
                    didnt_receive_email_button: Element = await self.tab.select(
                        'a[href="/account/access?lang=en&did_not_receive=true"]')
                    if didnt_receive_email_button:
                        await didnt_receive_email_button.click()
                        await asyncio.sleep(2)
                except asyncio.TimeoutError:
                    return True
            else:
                return False

    async def check_login_suspended(self) -> bool:
        try:
            await self.tab.find("Your account is suspended")
            return True
        except TimeoutError:
            return False

    async def check_login_still_login_page(self) -> bool:
        try:
            await self.tab.select('a[data-testid="signupButton"]', timeout=2)
            return True
        except TimeoutError:
            return False

    @retry(max_tries=3)
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

        # 6) Arkose challenge required
        # Your account has been locked message
        account_locked_page: bool = await self.check_login_account_locked()

        # no way to bypass captcha for now
        if account_locked_page:
            return 0

        # you should've returned something by now
        return -2

    # after login activities
    @retry(max_tries=3)
    async def get_homepage(self) -> Element:
        await self.tab.get('https://x.com')
        await self.check_got_it_message()
        await self.check_youre_in_control()
        return await self.tab.select("div[aria-label='Timeline: Your Home Timeline'] div[data-testid='cellInnerDiv']")

    @retry()
    async def scroll(self, lim=None) -> int:
        vertical_scroll: int = 0
        if lim is None:
            num_scroll = np.random.randint(10, 20)
        else:
            num_scroll = lim
        for i in range(num_scroll):
            rand_scroll_amount = np.random.randint(80, 120)
            await self.tab.scroll_down(rand_scroll_amount)
            vertical_scroll = vertical_scroll + rand_scroll_amount
            await asyncio.sleep(np.random.randint(10, 14) / 10)
        return vertical_scroll

    @retry(max_tries=2)
    async def follow_back(self, name="follow_back", progress: dict = None) -> bool:
        await self.check_got_it_message()
        if progress and name in progress.keys():
            progress[name]["attempts"] = progress[name]["attempts"] + 1

        await self.get_homepage()
        await self.tab.get(f"https://x.com/{self.username}/followers")

        # scroll
        await self.tab.scroll_down(25)
        await asyncio.sleep(random.randint(10, 22) / 10)

        # get followers
        not_following_list = []
        try:
            not_following_list: list[Element] = await self.tab.select_all(
                'div[data-testid="cellInnerDiv"] div[aria-label^="Follow @"]')
        except TimeoutError:
            pass
        followed: int = 0
        for f in not_following_list:
            if followed >= 3:
                break
            # randomly follow with 30/70% chance
            p = random.randint(1, 10) >= 7
            if p:
                await f.scroll_into_view()
                await f.click()
            followed += 1
            await asyncio.sleep(np.random.randint(10, 30) / 10)
        return True

    @retry(max_tries=2)
    async def retweet(self, name='retweet', num_retweets: int = 2, progress: dict = None) -> bool:
        await self.check_got_it_message()
        if progress and name in progress.keys():
            progress[name]["attempts"] = progress[name]["attempts"] + 1

        # get homepage
        await self.get_homepage()
        await asyncio.sleep(np.random.randint(10, 20) / 10)
        await self.scroll(3)

        # randomly find a retweet
        timeline_tweets_rt: list[Element] = await self.tab.select_all(
            "div[aria-label='Timeline: Your Home Timeline'] div[data-testid='cellInnerDiv'] button[data-testid='retweet']")
        tweet_rt_buttons: list[Element] = random.sample(timeline_tweets_rt,
                                                        k=min(num_retweets, len(timeline_tweets_rt)))

        for t in tweet_rt_buttons:
            await t.scroll_into_view()
            await t.click()

            # confirm retweet
            confirm_rt_dom = await self.tab.select("div[data-testid='retweetConfirm']")
            await asyncio.sleep(np.random.randint(7, 12) / 10)
            await confirm_rt_dom.click()
            await asyncio.sleep(np.random.randint(15, 20) / 10)
        return True

    @retry(max_tries=2)
    async def like(self, name="like", num_likes: int = 2, progress: dict = None) -> bool:
        await self.check_got_it_message()
        if progress and name in progress.keys():
            progress[name]["attempts"] = progress[name]["attempts"] + 1

        await self.get_homepage()
        await asyncio.sleep(np.random.randint(1, 20) / 10)
        await self.scroll()
        await asyncio.sleep(np.random.randint(15, 30) / 10)

        # randomly get a tweet & like
        timeline_tweets_like = await self.tab.select_all(
            "div[aria-label='Timeline: Your Home Timeline'] div[data-testid='cellInnerDiv'] button[data-testid='like']")
        tweet_like_buttons: list[Element] = random.sample(timeline_tweets_like,
                                                          k=min(num_likes, len(timeline_tweets_like)))
        for t in tweet_like_buttons:
            await t.scroll_into_view()
            await t.click()
        return True

    @retry(max_tries=2)
    async def view_trending(self, name="view_trending", progress: dict = None) -> bool:
        await self.check_got_it_message()
        if progress and name in progress.keys():
            progress[name]["attempts"] = progress[name]["attempts"] + 1

        # 1. get homepage
        await self.get_homepage()
        await asyncio.sleep(np.random.randint(1, 20) / 10)

        # 2. navigate to explore (randomly for you and explore)
        # href="/explore/tabs/for_you" -> click to get to explore for you
        try:
            butn = await self.tab.select('a[href="/explore/tabs/for_you"]')
            await butn.click()
        except Exception as e:
            await self.tab.get("https://x.com/explore/tabs/for_you")
        await asyncio.sleep(np.random.randint(1, 20) / 20)

        # (50% chance of only scrolling down explore)
        if np.random.randint(0, 10) > 5:
            # href="/explore/tabs/keyword" -> click to get trending in Canada
            butn = await self.tab.select('a[href="/explore/tabs/trending"]')
            await butn.click()
            await asyncio.sleep(np.random.randint(1, 20) / 20)

        # 3. scroll randomly
        await self.scroll(lim=2)

        return True

    @retry(max_tries=2)
    async def view_messages(self, name: str = "view_messages", progress: dict = None) -> bool:
        await self.check_got_it_message()
        if progress and name in progress.keys():
            progress[name]["attempts"] = progress[name]["attempts"] + 1

        await self.get_homepage()
        await asyncio.sleep(np.random.randint(10, 30) / 10)
        try:
            messages_dom = await self.tab.select('a[href="/messages"]')
            await messages_dom.click()
        except Exception as e:
            await self.tab.get("https://x.com/messages")
        await asyncio.sleep(np.random.uniform(10, 30) / 10)

        await self.scroll(lim=2)
        return True

    @retry(max_tries=2)
    async def view_homepage(self, name="view_homepage", progress: dict = None) -> bool:
        await self.check_got_it_message()
        await self.check_youre_in_control()
        if progress and name in progress.keys():
            progress[name]["attempts"] = progress[name]["attempts"] + 1

        # 1. get homepage
        await self.get_homepage()
        await asyncio.sleep(np.random.randint(1, 20) / 10)

        await self.scroll(lim=7)
        return True

    # utility methods
    async def check_got_it_message(self):
        try:
            got_it_button: Element = await (self.tab.find("Got it", best_match=True, timeout=5))
            if got_it_button:
                await got_it_button.click()
        except asyncio.exceptions.TimeoutError:
            pass

    async def check_youre_in_control(self):
        try:
            keep_less_relevant_ads: Element = await self.tab.find("Keep less relevant ads", best_match=True, timeout=5)
            if keep_less_relevant_ads:
                await keep_less_relevant_ads.click()
        except asyncio.exceptions.TimeoutError:
            pass
