import asyncio
import json
import random
from typing import Optional

from browserforge.fingerprints import FingerprintGenerator
from app.scraper.hti.twitter_api_client.account import Account
from app.scraper.hti.twitter_api_client.scraper import Scraper
from httpx import AsyncClient
from app.scraper.twscrape.api import API
import numpy as np


class TwitterSession:
    def __init__(self,
                 username: str,
                 password: str,
                 email: str,
                 cookies: str | dict | None = None,
                 session: AsyncClient | None = None):
        self.email: str = email
        self.username = username
        self.password = password
        self.cookies = cookies
        self.account: Optional[Account] = Account(
            email=self.email,
            password=self.password,
            username=self.username,
            cookies=self.cookies
        )
        self.scraper: Optional[Scraper] = Scraper(
            email=self.email,
            username=self.username,
            password=self.password,
            cookies=self.cookies
        )

    async def login(self) -> bool | None:
        login_ = Account(self.username, self.password)(
            email=self.email,
            username=self.username,
            password=self.password,
            cookies=self.cookies,
            session=self.session
        )
        if login_:
            return True
        else:
            return False

    async def follow_back(self):
        #SCRATCH
        pass

    async def retweet(self) -> bool:
        homepage_tweets: list[dict] = await self.view_homepage()
        # randomly pick 2 and retweet
        two_tweets_random = random.sample(homepage_tweets, min(2, len(homepage_tweets)))
        for tweet in two_tweets_random:
            twid = tweet["id"]
            await self.account.asyncRetweet(twid)
            await asyncio.sleep(abs(np.random.normal(3, 0.5)))
        return True

    async def like(self):
        homepage_tweets: list[dict] = await self.view_homepage()
        # randomly pick 2 and retweet
        two_tweets_random = random.sample(homepage_tweets, min(2, len(homepage_tweets)))
        for tweet in two_tweets_random:
            twid = tweet["id"]
            await self.account.asyncLike(twid)
            await asyncio.sleep(abs(np.random.normal(3, 0.5)))
        return True

    async def view_messages(self) -> dict:
        # DONE
        account_dms = await self.account.asyncDmHistory()
        return account_dms

    async def view_trending(self):
        # SCRATCH
        pass

    async def view_homepage(self) -> list[dict]:
        # DONE
        timeline = await self.account.asyncHomeLatestTimeline(limit=20)
        timeline = timeline.pop()
        timeline_tweets = parse_timeline(timeline)
        return timeline_tweets


async def main():
    """api = API(pool="/Users/mikad/MEOMcGill/twitter_scraper/database/accounts.db")
    accounts = await api.pool.get_active()
    for account in accounts:"""
    cookies = {"night_mode": "2", "guest_id_marketing": "v1%3A173463410391993355",
               "guest_id_ads": "v1%3A173463410391993355", "personalization_id": "\"v1_6thhTB1cLarRuXNW/zjT1A==\"",
               "guest_id": "v1%3A173463410391993355", "kdt": "ZW4ZxGwHOYyrC5HKR45oclrsIsw8yITikDvJp0RG", "lang": "en",
               "twid": "u%3D1539352389278367744",
               "_cfuvid": "2Tcy_EqLJ0qhYnviOGCTm_1P9sZBMu9.hAx5FSqz2z4-1717104076479-0.0.1.1-604800000",
               "timestamp": "173712500852398", "gt": "1881846928758985090",
               "auth_token": "418adcfa8e9af4fc72c9b4dd1abe4c28fbd9f29a",
               "ct0": "0c8adbaf113bb546544e60704e8363624d3153b1ff8874e0591617dbd2c9fee61a194e19ef6697a21103b9e264086bb5320e4d1e8b78a4467de7d1df504afa63d5b32cd2d9ff5568b81bbba55af8c179",
               "IDE": "AHWqTUmvQOxzAna1XO406hfX2OW9OfXPUsWDYqAuyR8-DHr06OEKRGeHToLYNBDe68Q",
               "NID": "520=qeQZc9Myh5-wV1R48z28mbP80B37cxugAR7xB6peX2XSCWXJlkCeoDsSBmQqHLPRah9KEInIEW8AdU1yEbgcPhl8WrVWtAm_9a9O9DKDDHAX38mBh8Tt9qMrgvVSAaMHORIzBAA6w6u-9KvsSh5bvvhsANkPEIO1K9FIEpTqTglZi2B8A0_vjDwomGGoEi9bwO3B"}
    ts = TwitterSession(
        username="double00flumpy",
        password="Azertyuiop180600!",
        email="flumpy180600@gmail.com",
        cookies=cookies)
    await ts.login()
    await ts.view_messages()
    pass


if __name__ == "__main__":
    f = FingerprintGenerator().generate()
    asyncio.run(main())
