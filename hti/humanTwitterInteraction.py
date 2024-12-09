# local package
from my_utils.logger.logger import logger

# local
from twitterActions import TwitterActions
from twscrape.account import Account

import os
import random
import numpy as np
from asyncio.exceptions import TimeoutError
from asyncio import iscoroutine, iscoroutinefunction
import asyncio
import json
import logging
import itertools
from typing import Callable, Any, Awaitable
import httpx
import sqlite3
import datetime
import re
from dataclasses import dataclass, asdict
from dotenv import load_dotenv

from playwright.async_api import async_playwright, Playwright
from camoufox import AsyncCamoufox

# custom type
AsyncCallable = Callable[[Any, Any], Awaitable[Any]]


# session functions
def process_cookies_in(cookies: dict | str, url: str = "https://x.com") -> list[dict]:
    if isinstance(cookies, str):
        cookies = json.loads(cookies)
    processed_cookies = [
        {
            "url": url,
            "name": k,
            "value": v
        } for k, v in cookies.items()
    ]
    return processed_cookies


def process_cookies_out(cookies: list[dict], url: str | None = ".x.com") -> dict:
    # Todo: put it in dict format: everything needs to be a string, same format as accounts.db
    out_cookies = {str(c["name"]): str(c["value"]) for c in cookies if url and c["domain"] == url}
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
                 headless=True,
                 cookies: str | dict = None):

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
            self.cookies: list[dict] = process_cookies_in(cookies)  # assume in the format of Twitter related cookies
        else:
            self.cookies = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_t, exc_v, exc_tb):
        if exc_t or exc_v or exc_tb:
            logger.error(f"{exc_t}, {exc_v}, {exc_tb}")

    async def run_interaction(self, size: int = None) -> HTIOutput:
        logger.info(f"{self.username}: starting default interaction")
        start = datetime.datetime.now()

        # start the async context
        async with AsyncCamoufox() as browser:
            context = await browser.new_context(viewport={"width": 1024, "height": 768})

            if self.cookies:
                # add cookies to the context
                await context.add_cookies(self.cookies)

            # page
            page = await context.new_page()
            tw_actions = TwitterActions(
                page=page,
                username=self.username)

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
                # do things in list
                for func_name, func_callable in activities.items():
                    kwargs: dict = {"progress": progress,
                                    "name": func_name}
                    res: bool = await func_callable(**kwargs)
                    progress[func_name]["success"] = True if res == 1 else False
                # retrieve the session cookies and everything
                self.cookies: list[dict] = await context.cookies("https://x.com")
                pass

        end = datetime.datetime.now()
        hti_output: HTIOutput = HTIOutput(
            username=self.username,
            start_time=start.strftime('%Y-%m-%d %H:%M:%S'),
            end_time=end.strftime('%Y-%m-%d %H:%M:'),
            total_time=(end - start).total_seconds(),
            login_status=log,
            activities=progress,
            cookies=process_cookies_out(self.cookies) if self.cookies else {},
            humanization_status=True if log == 1 else False
        )
        return hti_output

    # support functions
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


async def humanize(acc: Account) -> HTIOutput:
    async with HumanTwitterInteraction(
            username=acc.username,
            password=acc.password,
            email=acc.email,
            email_password=acc.email_password
    ) as hti:
        res: HTIOutput = await hti.run_interaction()
    return res


async def main():
    async with HumanTwitterInteraction(
            username="double00flumpy",
            password="Azertyuiop180600!",
            email="flumpy180600@gmail.com",
            email_password="Mika180600!",
            cookies={}
    ) as hti:
        res: HTIOutput = await hti.run_interaction()
        res: dict = res.dict()
        pass
        # maybe add something to update the accounts.db cookies column


if __name__ == "__main__":
    asyncio.run(main())
