# local
from twscrape.api import API
from twscrape.utils import gather
from utils.asyncHumanTaskManager import AsyncHumanTaskManager
from utils.exceptions import NoAccountsAvailable
from utils.upload_to_s3 import upload_to_s3
from utils.folder_manipulation import save_to_jsonl
from utils.logger import logger
from utils.meo_api import update_crawler_history
from utils.accounts import fetch_accounts
from hti import HumanTwitterInteraction

from asyncio import Queue, Future, Task
import asyncio
from typing import Callable, Any
import time
import os
from dotenv import load_dotenv
import requests
import datetime
from thefuzz import fuzz


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
                logger.warning(f"SeedID set to 0 for tweet {tweet['user']['username'].lower()} and seed {handle.lower()} with ratio {ratio}")
            elif ratio > 80:
                logger.error(f"SeedID set to 0 for tweet {tweet['user']['username'].lower()} and seed {handle.lower()} with ratio {ratio}")
            new_format = {
                "phh_id": "0",
                "seed_id": "0",
                "crawled_date": crawled_date,  # strftime("%Y-%m-%dT%H:%M:%SZ")
                "collection": "n/a" if not force_collection else collection,
                "data": tweet
            }
        tweets_new_format.append(new_format)
    return tweets_new_format


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

        if self.use_case is not None:
            # set lim_acc to the number of active accounts
            self.active_accounts: list[dict] = fetch_accounts(
                query=f"""SELECT * FROM accounts WHERE active=true AND use_case={use_case}""",
                path=os.path.join(self.path_library_folder, "accounts.db")
            )
        else:
            self.active_accounts: list[dict] = fetch_accounts(
                query=f"""SELECT * FROM accounts WHERE active=true""",
                path=os.path.join(self.path_library_folder, "accounts.db")
            )

        substract = (len(self.active_accounts) - 5)
        substract = substract if substract >= 0 else lim_acc
        self.lim_acc = min(lim_acc, substract)
        self.lim_browser = min(lim_browser, self.lim_acc)

        # instantiate when you scrape
        self.api = None
        self.asyncTaskMan_browser = None
        self.asyncTaskMan_acc = None

        logger.info(f"Active accounts {len(self.active_accounts)}: {[i['username'] for i in self.active_accounts]}")
        logger.info(f"Number of worker accounts: {self.lim_acc}")
        logger.info(f"Number of browser instances: {self.lim_browser}")

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
                                       bool_change_to_new_format: bool = True) -> None:
        data = await self.search_twscrape(query=query, limit=limit)
        logger.info(f"Saving data from {query} to {path} with new_format set to {bool_change_to_new_format}, s3 {bool_upload_to_s3} and update_phh_history {update_phh_history}.")
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

    async def user_by_login_twscrape(self, username: str):
        return await self.api.user_by_login(username)

    async def user_tweets_and_replies_twscrape(self, uid: int):
        return await self.api.user_tweets_and_replies(uid)

    async def user_tweets_twscrape(self, uid: int):
        return await self.api.user_tweets(uid)

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

    async def _process_search_results(self, t: Task, rand_name_to_filename: dict[str, str]) -> None:
        """
        Process search results for Twitter advanced search
        Parameters
        ----------
        t : Future
            a Future object that is marked done
        rand_name_to_filename : dict['str', 'str']
            a dictionary which holds the filename and path in which to store the collected tweets.

        Returns
        -------
        None
        """
        t_name = t.get_name()
        t_results = [i.dict() for i in t.result()]  # get the results
        file_name = rand_name_to_filename[t_name]
        await self.asyncTaskMan_acc.remove_task(t_name)

        if file_name:
            save_to_jsonl(file_name, t_results)
        else:
            logger.warning(f"Unable to save {len(t_results)} from {t_name} because filename is {file_name}.")

    async def run_queries_async(self,
                                queries: Queue[dict],
                                func: Callable = None,
                                asyncTaskMan: AsyncHumanTaskManager = None,
                                default: bool = False) -> dict:
        """
        Run the queries stored in queries. The function is function and query agnostic so long
        as the function query input matches the query itself.
        Parameters
        ----------
        queries : Queue[tuple[dict, str]]
            a queue of tuples containing
                - dict: keyword arguments for the search function being called,
                - str: the path to where JSON serializable objects should be saved
        func : Callable
            a function which takes as input the queries in queries
        asyncTaskMan: AsyncHumanTaskManager, optional, default=None
            an instance of the AsyncHumanTaskManager if you want to run queries not with the account task manager
        default : bool, optional, default=False
            whether to use the default function in the asyncTaskManager
        Returns
        -------
            dict
        """
        if not default and func is None:
            raise ValueError("Argument default is False and fun is None,\ncannot run non-default task if function is "
                             "None.")
        if not asyncTaskMan:
            asyncTaskMan = self.asyncTaskMan_acc

        next_query: dict | None = None

        meta_data: dict = {
            "total_num_queries": queries.qsize(),
            "num_queries": 0
        }
        results: list[dict[str, Any]] = []
        task_name_to_query: dict[str, Any] = {}
        while True:
            try:
                accept_next_task: bool = True
                while accept_next_task:
                    if next_query is None:
                        next_query = queries.get_nowait() # this is just for the first run
                    if default:
                        task_name = asyncTaskMan.add_default_task(**next_query)
                    else:
                        task_name = asyncTaskMan.add_task(async_task=func,
                                                          **next_query)
                    if task_name:
                        task_name_to_query[task_name] = next_query
                        next_query = queries.get_nowait()
                    else:
                        accept_next_task = False
                done, pending = await asyncTaskMan.run_all_tasks(asyncio.FIRST_COMPLETED)
                for t in done:
                    meta_data["num_queries"] += 1
                    results.append({"args": task_name_to_query[t.get_name()],  # test t.task_name
                                    "results": t.result()})
                    task_name_to_query.pop(t.get_name(), None)

                    t_name = t.get_name()
                    await asyncTaskMan.remove_task(t_name)
            except NoAccountsAvailable as e:
                logger.error(e.message)
                break
            except asyncio.QueueEmpty:
                # all accounts have been scraped
                done, pending = await asyncTaskMan.run_all_tasks(asyncio.ALL_COMPLETED)
                for t in done:
                    # await self._process_search_results(t, rand_name_to_filename)
                    meta_data["num_queries"] += 1
                    results.append({"args": task_name_to_query[t.get_name()],
                                    "results": t.result()})
                    task_name_to_query.pop(t.get_name(), None)
                    if default:
                        t_name = t.get_name()
                        await asyncTaskMan.remove_task(t_name)
                break
        meta_data["results"] = results
        return meta_data

    async def instantiate_twscrape_api(self):
        """
        Instantiate the account and browser task managers used respectively to organise
        query searches and browser usage.
        Returns
        -------
        None
        """
        # task managers & API instantiation
        task_instances = []
        for i in range(self.lim_browser):
            print(f"\tCreating browser {i}")
            temp = HumanTwitterInteraction(headless=self.headless)
            await temp.instantiate_browser()
            task_instances.append(temp)

        self.asyncTaskMan_acc: AsyncHumanTaskManager = AsyncHumanTaskManager(num_max_task=self.lim_acc)
        self.asyncTaskMan_browser: AsyncHumanTaskManager = AsyncHumanTaskManager(num_max_task=self.lim_browser,
                                                                                 task_instances=task_instances)
        self.api: API | None = API(atm_b=self.asyncTaskMan_browser,
                                   db_file=os.path.join(self.path_library_folder, "accounts.db"),
                                   use_case=self.use_case)
        for u in self.active_accounts:
            username = u["username"]
            await self.api.pool.mark_no_longer_in_use(username)

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

    async def login_to_all_accounts(self, use_case: int):
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

    async def search_scrape(self,
                            queries: list[dict],
                            use_case: int | None = 0
                            ) -> dict:
        logger.info(f"Scraping {len(queries)} queries")
        # scraping meta-data
        meta_data: dict = {
            "start_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        query_meta_data: dict = {}
        if len(queries) > 0:
            queries_queue = Queue()
            for q in queries:
                queries_queue.put_nowait(q)
            del queries

            # instantiate the API objects
            await self.instantiate_twscrape_api()

            # log into all accounts before starting
            await self.login_to_all_accounts(use_case=use_case)

            # run with the search method
            query_meta_data = await self.run_queries_async(func=self.search_twscrape_and_save,
                                                           queries=queries_queue,
                                                           default=False,
                                                           asyncTaskMan=None)

        # update meta_data
        meta_data["end_time"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # get status of the active accounts after the query
        query = """SELECT * FROM accounts WHERE username IN ({0})""".format(
            ', '.join('?' for _ in self.active_accounts))
        accounts_after: list[dict] = fetch_accounts(
            query=query,
            params=[i["username"] for i in self.active_accounts],
            path=os.path.join(self.path_library_folder, "accounts.db")
        )
        meta_data["post_scraping_accounts"] = {dic["username"]: dic["active"] for dic in accounts_after}
        meta_data["query_meta_data"] = query_meta_data

        # output scraping metadata
        return meta_data

