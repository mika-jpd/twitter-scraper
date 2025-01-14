# local
from twscrape.api import API
from twscrape.utils import gather
from twscrape.account import Account
from twscrape.db import fetchall, fetchone, execute
from twscrape import API, gather, Tweet
from twscrape.models import parse_tweets
from my_utils.upload_to_s3 import upload_to_s3
from my_utils.upload_to_s3.upload_to_s3 import upload_to_s3
from my_utils.folder_manipulation import save_to_jsonl
from my_utils.logger import logger
from my_utils.meo_api import update_crawler_history
from hti import HumanTwitterInteraction, HTIOutput

from asyncio import Queue, Future, Task, Semaphore
import asyncio
from typing import Callable, Any, Iterable, Awaitable
import time
import os
from dotenv import load_dotenv
import requests
import datetime
from thefuzz import fuzz
import random

AsyncCallable = Callable[[Any, Any], Awaitable[Any]]


def change_to_new_format(data: list[dict], seed_info: dict, force_collection: bool = False) -> list[dict]:
    """phh_id_collection_handle = filename.split("/")[-1]
        phh_id_collection_handle = phh_id_collection_handle.split('_')[:-2]
        phh_id = phh_id_collection_handle[0]
        seed_id = phh_id_collection_handle[1]
        collection = phh_id_collection_handle[2].replace("-", "_")
        handle = '_'.join(phh_id_collection_handle[3:])"""

    phh_id = seed_info["ID"]
    seed_id = seed_info["SeedID"]
    collection = seed_info["Collection"]
    handle = seed_info["Handle"]

    tweets_new_format: list[dict] = []

    for raw_tweet in data:
        if "phh_id" in raw_tweet.keys():
            tweet = raw_tweet["data"]
        else:
            tweet = raw_tweet

        crawled_date: str = tweet["date_scrape"].replace("Z", "")
        crawled_date: str = crawled_date.replace(" ", "T")
        crawled_date = crawled_date + "Z"
        is_not_a_retweet_nor_a_quote = (handle.lower() == tweet["user"]["username"].lower())
        if is_not_a_retweet_nor_a_quote:
            new_format = {
                "phh_id": str(phh_id),
                "seed_id": str(seed_id),
                "crawled_date": crawled_date,  # strftime("%Y-%m-%dT%H:%M:%SZ")
                "collection": collection,
                "data": tweet
            }
            tweets_new_format.append(new_format)
        else:
            ratio = fuzz.ratio(tweet['user']['username'].lower(), handle.lower())
            if ratio > 70 and ratio < 80:
                logger.warning(
                    f"SeedID set to 0 for tweet {tweet['user']['username'].lower()} and seed {handle.lower()} with ratio {ratio}")
            elif ratio > 80:
                logger.error(
                    f"SeedID set to 0 for tweet {tweet['user']['username'].lower()} and seed {handle.lower()} with ratio {ratio}")
            new_format = {
                "phh_id": "0",
                "seed_id": "0",
                "crawled_date": crawled_date,  # strftime("%Y-%m-%dT%H:%M:%SZ")
                "collection": "n/a" if not force_collection else collection,
                "data": tweet
            }
        tweets_new_format.append(new_format)
    return tweets_new_format


def get_user_tweets_only(tweets: list[Tweet]) -> list[Tweet]:
    # remove original retweet/quote tweets by the same user
    rt_tweets = [t.retweetedTweet.id for t in tweets if t.retweetedTweet]
    qt_tweets = [t.quotedTweet.id for t in tweets if t.quotedTweet]

    tweets = [t for t in tweets if (t.id not in rt_tweets) and (t.id not in qt_tweets)]

    # remove the pinned tweet (there's a better way to do this)
    if len(tweets) > 1:
        pinned_tweets = random.sample(tweets, 1).pop().user.pinnedIds
        tweets = [t for t in tweets if t.id not in pinned_tweets]
    return tweets


class TwitterScraper:
    def __init__(self,
                 lim_acc: int,
                 lim_browser: int,
                 setup_path: str,
                 use_case: int | None,
                 path_library_folder: str,
                 headless: bool = False):

        self.headless = headless
        self.use_case = use_case

        # paths
        self.path_library_folder = path_library_folder
        self.path_setup = setup_path

        # can probably just use the pool methods for this...
        if self.use_case is not None:
            # set lim_acc to the number of active accounts
            self.active_accounts: list[dict] = asyncio.run(
                fetchall(
                    query=f"""SELECT * FROM accounts WHERE active=true AND use_case={use_case}""",
                    path=os.path.join(self.path_library_folder, "accounts.db")
                )
            )
        else:
            self.active_accounts: list[dict] = asyncio.run(
                fetchall(
                    query=f"""SELECT * FROM accounts WHERE active=true""",
                    path=os.path.join(self.path_library_folder, "accounts.db")
                )
            )

        try:
            # there's already an event loop
            event_loop = asyncio.get_running_loop()
            event_loop.run_until_complete(self.instantiate_twscrape_api())
        except RuntimeError:
            # no event loop running
            asyncio.run(self.instantiate_twscrape_api())

        substract = (len(self.active_accounts) - 5)
        substract = substract if substract >= 0 else lim_acc
        self.lim_acc = min(lim_acc, substract)
        self.lim_browser = min(lim_browser, self.lim_acc)
        self.sem: asyncio.Semaphore = asyncio.Semaphore(self.lim_browser)

        # instantiate when you scrape
        self.api = None
        self.asyncTaskMan_browser = None
        self.asyncTaskMan_acc = None

        logger.info(f"Active accounts {len(self.active_accounts)}: {[i['username'] for i in self.active_accounts]}")
        logger.info(f"Number of worker accounts: {self.lim_acc}")
        logger.info(f"Number of browser instances: {self.lim_browser}")

    async def save(self,
                   data: Iterable,
                   path: str,
                   seed_info: dict,
                   start_date: str,
                   end_date: str,
                   update_phh_history: bool = True,
                   force_collection: bool = False,
                   bool_upload_to_s3: bool = True,
                   bool_change_to_new_format: bool = True
                   ):
        logger.info(
            f"Saving data from {seed_info['Handle']} to {path} with new_format set to {bool_change_to_new_format}, s3 {bool_upload_to_s3} and update_phh_history {update_phh_history}.")
        data = [t.dict() if not isinstance(t, dict) else t for t in data]

        if bool_change_to_new_format:
            data = change_to_new_format(data=data, seed_info=seed_info, force_collection=force_collection)

        # save locally
        save_to_jsonl(path=path, data=data)

        if bool_upload_to_s3:
            # upload to s3
            upload_to_s3(
                bucket_name=os.getenv("S3BUCKET"),
                folder=os.getenv("S3FOLDER"),
                filepath=path
            )
        if update_phh_history:
            # update seed history
            phh_id: str = str(seed_info["ID"])
            update_crawler_history(
                phh_id=phh_id,
                start_date=start_date,
                end_date=end_date
            )
        return data

    async def search_twscrape_and_save(self,
                                       query: str,
                                       path: str,
                                       seed_info: dict,
                                       start_date: str,
                                       end_date: str,
                                       limit: int = -1,
                                       update_phh_history: bool = True,
                                       force_collection: bool = False,
                                       bool_upload_to_s3: bool = True,
                                       bool_change_to_new_format: bool = True,
                                       sem: asyncio.Semaphore = None) -> list[dict] | list:

        if sem:
            async with sem:
                data = await self.search_twscrape(query=query, limit=limit)
        else:
            data = await self.search_twscrape(query=query, limit=limit)
        data = await self.save(data=data,
                               path=path,
                               seed_info=seed_info,
                               start_date=start_date,
                               end_date=end_date,
                               update_phh_history=update_phh_history,
                               force_collection=force_collection,
                               bool_upload_to_s3=bool_upload_to_s3,
                               bool_change_to_new_format=bool_change_to_new_format)
        return data

    async def user_tweets_and_replies_twscrape_and_save(self,
                                                        uid: int,
                                                        path: str,
                                                        seed_info: dict,
                                                        start_date: str,
                                                        end_date: str,
                                                        stopping_condition: Callable = None,
                                                        update_phh_history: bool = True,
                                                        force_collection: bool = False,
                                                        bool_upload_to_s3: bool = True,
                                                        bool_change_to_new_format: bool = True,
                                                        sem: asyncio.Semaphore = None) -> list[dict] | list:
        if sem:
            async with sem:
                data, flag = await self.user_tweets_and_replies_twscrape(uid, stopping_condition)
        else:
            data, flag = await self.user_tweets_and_replies_twscrape(uid, stopping_condition)
        # check if flag went off
        if stopping_condition and len(data) > 0:  # triggers if you scraped all the tweets in a person's timeline (1. there are tweets) & you haven't gone far back enough (2. those tweets didn't trigger stopping condition)
            if not flag:  # then you have to do some work
                # figure out the earliest tweet that you have
                user_tweets: list[Tweet] = get_user_tweets_only(data)
                # launch a search_scrape with query the start date till the latest date you have + 1
                min_date: datetime.datetime = min(user_tweets, key=lambda x: x.date).date
                min_date = min_date + datetime.timedelta(days=1)
                end = min_date.strftime("%Y-%m-%d")

                query = f'from:{seed_info["Handle"]} include:nativeretweets include:retweets until:{end} since:{start_date}'
                async with sem:
                    data = await self.search_twscrape(
                        query=query,
                        limit=-1
                    )
        # filter out the tweets that are too new or too old by
        user_tweets: list[Tweet] = get_user_tweets_only(data)
        user_excluded_tweets: list[Tweet] = [
            d for d in user_tweets
            if datetime.datetime.strptime(start_date, "%Y-%m-%d") <= d.date < datetime.datetime.strptime(end_date,
                                                                                                         "%Y-%m-%d")
        ]  # 1. get the user tweets that are too new or too old
        excluded_tweets = []
        for t in user_excluded_tweets:
            if t.retweetedTweet is not None:  # 2.1 remove any corresponding retweetedTweet
                excluded_tweets.append(int(t.retweetedTweet.id))
            if t.quotedTweet is not None:  # 2.2 remove any corresponding quotedTweet
                excluded_tweets.append(int(t.quotedTweet.id))
            excluded_tweets.append(int(t.id))

        data = [d for d in data if not int(d.id) in excluded_tweets]
        data = await self.save(data=data,
                               path=path,
                               seed_info=seed_info,
                               start_date=start_date,
                               end_date=end_date,
                               update_phh_history=update_phh_history,
                               force_collection=force_collection,
                               bool_upload_to_s3=bool_upload_to_s3,
                               bool_change_to_new_format=bool_change_to_new_format)
        return data

    async def user_by_login_twscrape(self, username: str):
        return await self.api.user_by_login(username)

    async def user_tweets_and_replies_twscrape(self,
                                               uid: int,
                                               stopping_condition: Callable = None) -> tuple[list, bool]:
        logger.info(f'user tweets and replies for uid: {uid}')
        flag = False
        data = await gather(self.api.user_tweets_and_replies(uid, stopping_condition=stopping_condition, flag=flag))

        # process tweets if needed
        return data, flag

    async def user_tweets_twscrape(self, uid: int):
        logger.info(f'user tweets for uid: {uid}')
        data = await gather(self.api.user_tweets(uid))
        return data

    async def search_twscrape(self,
                              query: str,
                              limit: int = -1) -> list:
        """
        Calls the twscrape search function to perform advanced searches on Twitter
        Parameters
        ----------
        query : str
            Search query in the Twitter advanced search format
        limit : int, optional
            Limit the number of tweets returned by the search, by default -1 meaning no limit is used

        Returns
        -------
        list
            a list of dictionaries of each individual tweet returned by the advanced search
        """
        logger.info(f'searching query: {query}')
        data = await gather(self.api.search(query, limit))
        return data

    async def mark_active_accounts_not_in_use(self):
        """Mark all active accounts in accounts.db not in use."""
        # Todo: eventually won't need to do this as there will never be accounts that are wrongly marked in use
        if self.api:
            for u in self.active_accounts:
                username: str = u["username"]
                await self.api.pool.mark_no_longer_in_use(username=username)
        else:
            raise ValueError("API not initialized ! Please initialize Twscrape API before running this.")

    async def instantiate_twscrape_api(self):
        """
        Instantiate the account and browser task managers used respectively to organise
        query searches and browser usage.
        Returns
        -------
        None
        """
        # task managers & API instantiation

        self.api: API | None = API(pool=os.path.join(self.path_library_folder, "accounts.db"),
                                   use_case=self.use_case,
                                   sem=self.sem)
        self.active_accounts = await self.api.pool.get_active(use_case=self.use_case)
        for u in self.active_accounts:
            username = u.username
            await self.api.pool.set_in_use(username=username, in_use=False)

    @staticmethod
    def twitter_api_request(url: str, params: dict) -> list[dict] | None:
        """
        Parameters
        ----------
        url : str
            url of the API endpoint you're requesting
        params : dict
            parameters of the API endpoint request
        Returns
        -------
        list
            a list of the tweets returned by the request
        """
        load_dotenv()
        # Retrieve the Bearer Token from the environment variable
        bearer_token = os.getenv("TWITTER_BEARER_TOKEN")
        if not bearer_token:
            logger.error("Bearer Token not found in environment variables.")
            return None

        headers = {
            "Authorization": f"Bearer {bearer_token}"
        }

        # Include all possible tweet fields
        tweet_fields = (
            "attachments,author_id,context_annotations,conversation_id,created_at,"
            "entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,"
            "public_metrics,referenced_tweets,reply_settings,source,text,withheld,note_tweet"
        )
        params["tweet.fields"] = tweet_fields

        all_tweets: list[dict] = []
        next_token = None
        while True:
            if next_token:
                params["next_token"] = next_token

            time.sleep(1)
            response = requests.get(url=url, headers=headers, params=params)

            if response.status_code == 200:
                # Parse the JSON response
                tweets: dict = response.json()

                # Append tweets to the list
                if "data" in tweets:
                    data = tweets["data"]
                    all_tweets.extend(data)

                # Check if there's a next token for pagination
                next_token = tweets.get("meta", {}).get("next_token", None)

                # If no next token, break the loop
                if not next_token:
                    break
            elif response.status_code == 429:
                logger.error("Rate limit exceeded, waiting 15 seconds")
                logger.error(response.text)
                time.sleep(60)
            else:
                logger.error(f"Error: {response.status_code}")
                logger.error(response.text)
                break
        return all_tweets

    def tweet_lookup_api(self, tweet_id: str, output_path: str):

        # Set up the endpoint URL and headers
        url = "https://api.twitter.com/2/tweets"

        params = {
            "ids": tweet_id
        }
        tweets = self.twitter_api_request(url=url, params=params)
        save_to_jsonl(output_path, tweets)

    def search_api_(self,
                    query: str,
                    start_time,
                    end_time,
                    output_path: str,
                    max_results_per_page: int = 10
                    ) -> None:
        start = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        logger.info(f"Searching with API {query} between {start_time} & {end_time} saving at {output_path}")

        # Set up the endpoint URL and headers
        url = "https://api.twitter.com/2/tweets/search/all"

        # Set up the parameters for the API request
        params = {"query": query,
                  "max_results": max_results_per_page,
                  "sort_order": "recency",
                  "start_time": start_time,
                  "end_time": end_time
                  }
        tweets = self.twitter_api_request(url=url, params=params)
        tweets = [t for t in tweets]
        end = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        logger.info(f"Fetched {len(tweets)} between {start} & {end}")
        save_to_jsonl(output_path, tweets)

    def search_api(self, queries: list[tuple[dict, str | None]]):
        for q in queries:
            query = q[0]
            path = q[1]
            query["output_path"] = path
            self.search_api_(**query)
        pass

    async def _login_to_all_accounts(self, use_case: int):
        if self.use_case is not None:
            queries: list[dict] = [
                {
                    'username': i["username"],
                    "password": i["password"],
                    "email": i["email"],
                    "email_password": i["email_password"],
                    "twofa_id": i["twofa_id"],
                    "size": 1
                } for i in self.active_accounts if i["use_case"] == use_case]
        else:
            queries: list[dict] = [
                {
                    'username': i["username"],
                    "password": i["password"],
                    "email": i["email"],
                    "email_password": i["email_password"],
                    "twofa_id": i["twofa_id"],
                    "size": 1
                } for i in self.active_accounts]
        queue_queries = Queue()
        for q in queries:
            queue_queries.put_nowait(q)
        meta_data = await self.run_queries_async(asyncTaskMan=self.asyncTaskMan_browser,
                                                 queries=queue_queries,
                                                 default=True)
        for m in meta_data["results"]:
            await self.api.pool.update_cookies(username=m["args"]["username"], cookies=m["results"]["cookies"])
            await self.api.pool.reset_num_calls(username=m["args"]["username"])
            if int(m["results"]["login_status"]) == 0:
                await self.api.pool.set_active(
                    username=m["args"]["username"],
                    error_msg="login status 0 when logging into all accounts before a search_scrape",
                    active=False
                )
            elif int(m["results"]["login_status"]) == -1:
                await self.api.pool.mark_suspended(
                    username=m["args"]["username"],
                    error_msg="login status -1 when logging into all accounts before a search_scrape")
            elif int(m["results"]["login_status"]) == -3:
                await self.api.pool.mark_inactive(
                    username=m["args"]["username"],
                    error_msg="login status -3 when logging into all accounts before a search_scrape")
        pass

    async def login_to_all_accounts(self):
        async def _login_to_account(sem: asyncio.Semaphore, acc: Account, size: int) -> HTIOutput:
            async with sem:
                hti = HumanTwitterInteraction(username=acc.username,
                                              password=acc.password,
                                              email=acc.email,
                                              email_password=acc.email_password,
                                              twofa_id=acc.mfa_code,
                                              cookies=acc.cookies)
                res = await hti.run_interaction(size=size)
                return res

        sem = asyncio.Semaphore(5)
        queries: list[dict] = [
            {"sem": sem, "acc": Account(**k), "size": 1} for k in self.active_accounts
        ]
        login_results: tuple[HTIOutput] = await asyncio.gather(*[_login_to_account(**q) for q in queries])

        for out in login_results:
            await self.api.pool.set_cookies(username=out.username, cookies=out.cookies)
            await self.api.pool.set_num_calls(username=out.username, num_calls=0)
            await self.api.pool.set_active(
                username=out.username,
                error_msg=f"login status {out.login_status} when logging into all accounts before a scrape",
                active=True if int(out.login_status) == 1 else False
            )
            await self.api.pool.set_last_login(username=out.username, last_login=out.login_status)

    async def search_queries_scrape(self, queries: list[dict]):
        return self.run_scraper(queries=queries, func=self.search_twscrape_and_save)

    async def user_tweets_scrape(self, queries: list[dict]):
        return self.run_scraper(queries=queries, func=self.user_tweets_and_replies_twscrape_and_save)

    async def run_scraper(self,
                          queries: list[dict],
                          func: Callable
                          ) -> dict:
        logger.info(f"Scraping {len(queries)} queries")
        # scraping meta-data
        meta_data: dict = {
            "start_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        query_meta_data: dict = {}
        if len(queries) > 0:
            # instantiate the API objects
            await self.instantiate_twscrape_api()

            # log into all accounts before starting
            await self.login_to_all_accounts()

            # run with the search method
            sem: asyncio.Semaphore = asyncio.Semaphore(self.lim_acc)
            scraping_results: tuple[list] = await asyncio.gather(*[func(**q, sem=sem) for q in queries])

            # update meta-data
            meta_data["total_tweets_collected"] = sum([len(i) for i in scraping_results])
            meta_data["total_num_queries"] = len(queries)

        # update meta_data
        meta_data["end_time"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # get status of the active accounts after the query
        query = """
        SELECT * FROM accounts WHERE username IN ({0})
        """.format(', '.join(f':{u}' for u in enumerate(self.active_accounts)))

        accounts_after = await fetchall(
            db_path=os.path.join(self.path_library_folder, "accounts.db"),
            qs=query,
            params={user["username"]: user["username"] for user in self.active_accounts}
        )

        meta_data["post_scraping_accounts"] = {dic["username"]: dic["active"] for dic in accounts_after}

        # output scraping metadata
        return meta_data
