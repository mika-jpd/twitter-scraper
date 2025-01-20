# local package
from scraper.my_utils.logger.logger import logger

# local
from scraper.hti.twitterActions import TwitterActions
from scraper.twscrape.account import Account

import numpy as np
import asyncio
import json
from typing import Callable, Any, Awaitable
import datetime
from dataclasses import dataclass, asdict
from zendriver import Browser
from zendriver.cdp.network import CookieParam
from fake_useragent import UserAgent
import zendriver
# custom type
AsyncCallable = Callable[[Any, Any], Awaitable[Any]]


def process_cookies_in(cookies: dict | str, url: str = "https://x.com") -> list[CookieParam]:
    if isinstance(cookies, str):
        cookies = json.loads(cookies)
    processed_cookies = [
        {
            "url": url,
            "name": k,
            "value": v
        } for k, v in cookies.items()
    ]
    processed_cookies = [CookieParam(**c) for c in processed_cookies]
    return processed_cookies


def process_cookies_out(cookies: list[dict], url: list[str] | None = None) -> dict:
    if url is None:
        url = [".x.com", ".twitter.com"]
    out_cookies = {str(c["name"]): str(c["value"]) for c in cookies if "domain" in c and c["domain"] in url}
    return out_cookies


@dataclass
class HTIOutput:
    username: str
    start_time: str
    end_time: str
    total_time: float
    login_status: int
    activities: dict
    cookies: dict
    humanization_status: bool

    def dict(self) -> dict:
        return asdict(self)


class HumanTwitterInteraction:
    def __init__(self,
                 username: str,
                 password: str,
                 email: str,
                 email_password: str,
                 twofa_id: str = None,
                 browser_path: str | None = None,
                 headless: bool | str = True,
                 cookies: str | dict = None,
                 headers: dict[str, str] = None):

        # Twitter actions essentials
        self.headless = headless
        self.browser_path = browser_path

        # Init the username, password etc...
        self.username: str = username
        self.password: str = password
        self.email: str = email
        self.email_password: str = email_password
        self.twofa_id: str = twofa_id

        # cookies
        if cookies is not None:
            self.cookies: list[CookieParam] = process_cookies_in(
                cookies)  # assume in the format of Twitter related cookies
        else:
            self.cookies = None
        self.headers = headers

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_t, exc_v, exc_tb):
        if exc_t or exc_v or exc_tb:
            logger.error(f"{exc_t}, {exc_v}, {exc_tb}")

    async def instantiate_browser(self) -> Browser:
        # start the async context
        user_agent: str = self.headers["User-Agent"] if self.headers and "User-Agent" in self.headers else None
        if user_agent is None:
            ua = UserAgent()
            self.user_agent = ua.chrome
        driver = await zendriver.start(
            browser_path=self.browser_path,
            headless=self.headless,
            sandbox=False,
            browser_args=[
                f'--user-agent={self.user_agent}',
                "--disable-gpu",
                "--disable-dev-shm-usage",
                "--disable-dev-tools",
                "--no-sandbox"
            ]
        )
        return driver

    async def run_interaction(self, size: int = None) -> HTIOutput:
        logger.info(f"{self.username}: starting default interaction")
        start = datetime.datetime.now()

        driver = await self.instantiate_browser()

        # add the cookies
        if self.cookies:
            await driver.cookies.set_all(self.cookies)

        # tab
        tab = await driver.get("https://x.com")
        tw_actions = TwitterActions(
            tab=tab,
            username=self.username
        )

        activities: dict[str, AsyncCallable] = self.generate_random_twitter_interaction(
            size=size,
            tw_actions=tw_actions)
        progress = {
            k: {
                "success": False,
                "attempts": 0
            } for k in activities
        }
        log = await tw_actions.login(username=self.username,
                                     password=self.password,
                                     email=self.email,
                                     email_password=self.email_password,
                                     twofa_id=self.twofa_id)
        logger.info(f"{self.username}: login status: {log}")
        if log == 1:
            # do things in list
            for func_name, func_callable in activities.items():
                kwargs: dict = {"progress": progress,
                                "name": func_name}
                res: bool = await func_callable(**kwargs)
                progress[func_name]["success"] = res
            # retrieve the session cookies and everything
            self.cookies = await driver.cookies.get_all()
            self.cookies = [c.to_json() for c in self.cookies]
            pass
        await driver.stop()
        end = datetime.datetime.now()
        hti_output: HTIOutput = HTIOutput(
            username=self.username,
            start_time=start.strftime('%Y-%m-%d %H:%M:%S'),
            end_time=end.strftime('%Y-%m-%d %H:%M:'),
            total_time=(end - start).total_seconds(),
            login_status=log if log is not None else -2,
            activities=progress,
            cookies=process_cookies_out(self.cookies) if self.cookies else {},
            humanization_status=True if log == 1 else False
        )
        return hti_output

    @staticmethod
    def generate_random_twitter_interaction(tw_actions: TwitterActions, size: int = None) -> dict:
        prob_of_each_activity = [0.10, 0.10, 0.10, 0.10, 0.35, 0.25]
        activity_name_to_func = {
            "follow_back": tw_actions.follow_back,
            "retweet": tw_actions.retweet,
            "like": tw_actions.like,
            "view_messages": tw_actions.view_messages,
            "view_trending": tw_actions.view_trending,
            "view_homepage": tw_actions.view_homepage
        }
        if not size:
            size = np.random.choice(a=[1, 2, 3], size=1, p=[0.20, 0.60, 0.20])
        rand_activity_name_list = np.random.choice(
            a=list(activity_name_to_func.keys()),
            size=size,
            p=prob_of_each_activity
        )

        activities = {f"{k}_{c}": activity_name_to_func[k] for c, k in enumerate(rand_activity_name_list)}
        return activities

async def humanize(acc: Account, size: int = None, headless: bool = True, sem: asyncio.Semaphore = None) -> HTIOutput:
    if sem:
        async with sem:
            async with HumanTwitterInteraction(
                    username=acc.username,
                    password=acc.password,
                    email=acc.email,
                    email_password=acc.email_password,
                    cookies=acc.cookies,
                    twofa_id=acc.mfa_code,
                    headers=acc.headers,
                    headless=headless,
                    browser_path="/usr/bin/google-chrome"
            ) as hti:
                res: HTIOutput = await hti.run_interaction(size=size)
    else:
        async with HumanTwitterInteraction(
                username=acc.username,
                password=acc.password,
                email=acc.email,
                email_password=acc.email_password,
                cookies=acc.cookies,
                twofa_id=acc.mfa_code,
                headers=acc.headers,
                headless=headless,
                browser_path="/usr/bin/google-chrome"
        ) as hti:
            res: HTIOutput = await hti.run_interaction(size=size)
    return res


async def main():
    async with HumanTwitterInteraction(
            username="double00flumpy",
            password="Azertyuiop180600!",
            email="flumpy180600@gmail.com",
            email_password="Mika180600!",
            cookies={},
            headless=False
    ) as hti:
        res: HTIOutput = await hti.run_interaction(size=1)
        res: dict = res.dict()
        pass


# testing
if __name__ == "__main__":
    asyncio.run(main())