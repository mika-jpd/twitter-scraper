# local
import os.path

from app.common.logger import get_logger
from app.scraper.hti.twitterActions_manual import TwitterActions

# pipped
import numpy as np
import asyncio
import json
from typing import Callable, Any, Awaitable
import datetime
from dataclasses import dataclass, asdict
from typing import Optional, Union
import platform

# zendriver
from zendriver import Browser
from zendriver.cdp.network import CookieParam
import zendriver as webdriver

# nodriver
"""from nodriver import Browser
from nodriver.cdp.network import CookieParam
import nodriver as webdriver"""

from fake_useragent import UserAgent

# custom type
AsyncCallable = Callable[[Any, Any], Awaitable[Any]]
logger = get_logger()


def process_cookies_in(cookies: Union[dict, str, list], url: str = "https://x.com") -> Optional[list[CookieParam]]:
    if isinstance(cookies, str):
        cookies: dict = json.loads(cookies)

    if isinstance(cookies, dict):
        processed_cookies: list[dict] = [
            {
                "url": url,
                "name": k,
                "value": v
            } for k, v in cookies.items()
        ]
    elif isinstance(cookies, list):
        processed_cookies: list[dict] = [
            {
                "url": url,
                "name": d["name"],
                "value": d["value"]
            } for d in cookies
        ]
    else:
        raise AttributeError("Cookies are not in the correct format")

    # check if ct0 and auth_token are there
    processed_cookies: list[CookieParam] = [CookieParam(**c) for c in processed_cookies]
    auth_tokens_present: bool = any([c.name == "ct0" for c in processed_cookies]) and any(
        [c.name == "auth_token" for c in processed_cookies])

    return processed_cookies if auth_tokens_present else None


def process_cookies_out(cookies: list[dict | CookieParam], url: list[str] | None = None) -> Optional[dict]:
    if url is None:
        url = [".x.com", ".twitter_api_client.com", "x.com", 'https://twitter.com', 'https://x.com']
    if all([isinstance(c, CookieParam) for c in cookies]):
        out_cookies = {
            str(c.name): str(c.value)
            for c in cookies
            if ("domain" in c.to_json() and c["domain"] in url) or ("url" in c.to_json() and c.url in url)
        }
    else:
        out_cookies = {str(c["name"]): str(c["value"]) for c in cookies if "domain" in c and c["domain"] in url}

    # check if contains auth tokens
    auth_tokens_present: bool = "ct0" in out_cookies.keys() and "auth_token" in out_cookies.keys()
    return out_cookies if auth_tokens_present else None


@dataclass
class HTIOutput:
    username: str
    start_time: str
    end_time: str
    total_time: float
    login_status: int
    activities: dict
    cookies: Optional[dict]
    humanization_status: bool

    def dict(self) -> dict:
        return asdict(self)


def get_chrome_path():
    if platform.system() == "Darwin":  # macOS
        path = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"  # path = os.path.expanduser("~/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
    else:  # Linux
        path = "/usr/bin/google-chrome-stable"  # "/usr/bin/google-chrome"
    return path


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
        self.driver: Browser | None = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_t, exc_v, exc_tb):
        if self.driver is not None:
            await self.driver.stop()

        if exc_t or exc_v or exc_tb:
            logger.error(f"{exc_t}, {exc_v}, {exc_tb}")

    async def instantiate_browser(self) -> Browser:
        driver = None
        ua = UserAgent(browsers="Chrome").random
        # start the async context
        for i in range(5):
            try:
                logger.debug(
                    f"{self.username}: attempt {i} to instantiate browser with headless {self.headless} and path {get_chrome_path()}")
                driver = await webdriver.start(
                    browser_executable_path=get_chrome_path(),
                    expert=True,
                    headless=self.headless,
                    sandbox=False,
                    browser_args=[
                        f'--user-agent={ua}',
                        "--disable-gpu",
                        "--disable-dev-shm-usage",
                        "--disable-dev-tools"
                    ]
                )
                break
            except Exception as e:
                logger.debug(f"{self.username}: exception {i} to instantiate browser with attributes {self.headless}")
                logger.debug(e)
                await asyncio.sleep(2)
                pass
        if driver:
            self.driver = driver
        else:
            raise Exception
        return driver

    async def run_interaction(self, size: int = None) -> HTIOutput:
        logger.info(f"{self.username}: starting default interaction")
        start = datetime.datetime.now()
        logger.debug(f"{self.username}: starting browser")
        driver = await self.instantiate_browser()

        # add the cookies
        if self.cookies:
            await driver.cookies.set_all(self.cookies)

        # tab
        logger.debug(f"{self.username}: getting x.com")
        tab = await driver.get("https://x.com")
        await tab
        tw_actions = TwitterActions(
            tab=tab,
            username=self.username,
            driver=driver
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
        if log == 1:
            logger.info(f"{self.username}: login status: {log}")
        else:
            logger.warning(f"{self.username}: login status: {log}")
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

        await driver.stop()
        self.driver = None
        end = datetime.datetime.now()
        hti_output: HTIOutput = HTIOutput(
            username=self.username,
            start_time=start.strftime('%Y-%m-%d %H:%M:%S'),
            end_time=end.strftime('%Y-%m-%d %H:%M:'),
            total_time=(end - start).total_seconds(),
            login_status=log if log is not None else -2,
            activities=progress,
            cookies=process_cookies_out(self.cookies) if self.cookies else None,
            humanization_status=True if log == 1 else False
        )
        return hti_output

    @staticmethod
    def generate_random_twitter_interaction(tw_actions: TwitterActions, size: int = None) -> dict:
        prob_of_each_activity = [0, 0, 0, 0.20, 0.40, 0.40]
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


from app.scraper.twscrape.account import Account


async def humanize(acc: Account, size: int = None, headless: bool = True, sem: asyncio.Semaphore = None) -> HTIOutput:
    if sem:
        async with sem:
            async with HumanTwitterInteraction(
                    username=acc.username,
                    password=acc.password,
                    email=acc.email,
                    email_password=acc.email_password,
                    cookies=acc.cookies,
                    twofa_id=acc.twofa_id,
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
                twofa_id=acc.twofa_id,
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
