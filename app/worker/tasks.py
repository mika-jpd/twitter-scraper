import traceback
import uuid

from app.scraper.TwitterScraper import TwitterScraper
from app.scraper.my_utils.meo_api.get_seeds import get_seeds
from app.scraper.my_utils.seed_manipulation.seeds import sort_seeds
from app.scraper.twscrape.models import Tweet, parse_tweets
from app.common.logger import setup_logging, get_logger, logger
from app.scraper.my_utils.dates import bin_and_tuple_date_range
from app.scraper.my_utils.queryModels import SearchQuery, TimelineQuery, ExploreQuery, SearchExploreQuery
from app.common.models.scraper_models import ConfigModel
from app.common.utils import get_project_root
from app.common.queues import scraper_queue, account_queue

from rq import Retry
from dateutil import tz
from rq.queue import Queue

import asyncio
from dotenv import load_dotenv
import datetime
import os
import json
import random
from typing import Callable, Optional


def date_stopping_condition(res, min_date) -> bool:
    min_date = datetime.datetime.strptime(min_date, "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc)
    tweets: list[Tweet] = [t for t in parse_tweets(res)]
    # remove original retweet/quote tweets by the same user
    rt_tweets = [t.retweetedTweet.id for t in tweets if t.retweetedTweet]
    qt_tweets = [t.quotedTweet.id for t in tweets if t.quotedTweet]

    tweets = [t for t in tweets if (t.id not in rt_tweets) and (t.id not in qt_tweets)]

    # remove the pinned tweet
    if len(tweets) > 1:
        pinned_tweets = random.sample(tweets[1:], 1).pop().user.pinnedIds
        tweets = [t for t in tweets if t.id not in pinned_tweets]

        # filter tweets by date
        tweets = [t for t in tweets if t.date < min_date]
        if len(tweets) > 0:
            return True
        else:
            return False
    else:
        return False


def generate_queries(seed_query: str,
                     scrape_method: str,
                     home_dir: str,
                     path_output_data: str,
                     start_date: Optional[str],
                     end_date: Optional[str],
                     date_ranges: list[tuple[str, str]]
                     ) -> list[SearchQuery | TimelineQuery]:
    logger = get_logger()
    handles = get_seeds(
        username=os.getenv('MEO_USERNAME'),
        password=os.getenv('MEO_PASSWORD'),
        query=seed_query
    )
    handles = sort_seeds(handles, path_setup=os.path.join(home_dir, "scraper", "my_utils", "seed_manipulation"))

    # TODO: TEMPORARY REFERENCE TO LOCAL twitter_handle.json -> user files !
    handle_to_id = {}
    not_found: list[str] = []
    if scrape_method == "timeline":
        path_handle_to_user_information_dir = os.path.join(home_dir,
                                                           "scraper",
                                                           "my_utils",
                                                           "seed_manipulation",
                                                           "handle_to_user_information")
        warning_msg: Optional[str] = None
        for h in handles:
            handle = h["Handle"]
            full_path = os.path.join(path_handle_to_user_information_dir, f"{handle}.json")
            if os.path.exists(full_path):
                tw_user_info = json.load(open(full_path, "r"))
                if "id_str" in tw_user_info:
                    handle_to_id[handle] = tw_user_info["id_str"]
                elif "Not found" in tw_user_info.values():
                    not_found.append(handle)
                else:
                    warning_msg: str = f"Parsing error while parsing handle {handle} with json\n{tw_user_info}"
            else:
                warning_msg: str = f"Unable to find the user info for handle {handle} in {path_handle_to_user_information_dir}"
            # send warning if there
            if warning_msg:
                logger.warning(warning_msg)
        logger.warning(f"{len(not_found)} users weren't found: {not_found}")
        # TODO: tell account for this !
        handles = [h for h in handles if h["Handle"] in handle_to_id.keys()]

    queries = []
    for h in handles:
        if scrape_method == "timeline":
            query = int(handle_to_id[h["Handle"]])  # or whatever you'll add as the twitter_api_client ID
            queries.append(
                {"query": query,
                 "path": os.path.join(path_output_data,
                                      f"{h['ID']}_"
                                      f"{h['SeedID']}_"
                                      f"{h['Collection'].replace('_', '-')}_"
                                      f"{h['Handle'].replace('_', '-')}_"
                                      f"{start_date}_{end_date}"
                                      f".jsonl"),
                 "seed_info": h,
                 "start_date": start_date,
                 "end_date": end_date,
                 "stopping_condition": lambda x: date_stopping_condition(x, start_date)
                 }
            )
        elif scrape_method == "search":
            for start, end in date_ranges:
                query = f'from:handle include:nativeretweets include:retweets until:{end} since:{start}'
                query = query.replace("handle", h["Handle"])
                queries.append(
                    {"query": query,
                     "path": os.path.join(path_output_data,
                                          f"{h['ID']}_"
                                          f"{h['SeedID']}_"
                                          f"{h['Collection'].replace('_', '-')}_"
                                          f"{h['Handle'].replace('_', '-')}_"
                                          f"{start_date}_{end_date}"
                                          f".jsonl"),
                     "seed_info": h,
                     "start_date": start_date,
                     "end_date": end_date
                     }
                )
        else:
            raise ValueError(f"Scrape method must either be timeline or search !")

    validated_queries = [SearchQuery(**q) if scrape_method == "search" else TimelineQuery(**q) for q in queries]
    return validated_queries


@logger.catch(reraise=True)
def scrape_twitter_(config: ConfigModel) -> dict:
    logger = get_logger()
    logger.info("Starting twitter scrape function.")
    # scraping method
    scrape_method = config.scrape_method
    use_case = config.use_case if config.use_case is not None else None  # config['scrape_type']['use_case']

    # dates
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    date_ranges: Optional[list[tuple[str, str]]] = None
    if config.seed_query:
        start_date = config.dates.start_date  # config['date']['start']
        end_date = config.dates.end_date  # config['date']['end']
        if not all([start_date, end_date]):  # REQUIRED
            raise KeyError("Need to provide start and end date !")

        start_date_dt = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        end_date_dt = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        if (end_date_dt - start_date_dt) > datetime.timedelta(days=5):
            date_ranges: list[tuple[str, str]] = bin_and_tuple_date_range(start_date_dt, end_date_dt)
        else:
            date_ranges: list[tuple[str, str]] = [(start_date, end_date)]

        if not (datetime.datetime.strptime(start_date, '%Y-%m-%d') < datetime.datetime.strptime(end_date, '%Y-%m-%d')):
            raise ValueError("Start date < end date.")  # also validated at config level
        data_dir_name: str = f"twitter_seed_{scrape_method}_{start_date}_{end_date}"
    else:  # means it's a custom query - checked at config level
        data_dir_name: str = f"twitter_custom_{scrape_method}_{config.custom_queries_dirname}"

    # acc & browser limits
    lim_acc = config.limit.accounts  # int(config['limit']['accounts'])
    lim_browser = config.limit.browsers  # int(config['limit']['browsers'])

    # query
    seed_query = config.seed_query  # config['query']['query']

    # home, output, logs & browser paths
    home_dir = config.paths.home if config.paths.home else os.path.join(get_project_root(),
                                                                        "app")  # config['paths']['home']
    path_output = config.paths.output if config.paths.output else os.path.join(get_project_root(),
                                                                               "output")  # config['paths']['output']
    path_browser = config.paths.browsers  # config['paths']['browser']
    data_dir = os.path.join("data", config.paths.data) if config.paths.data else os.path.join('data',
                                                                                              data_dir_name)  # config['path']['data']

    path_logs = os.path.join(path_output, 'logs')
    path_output_data = os.path.join(path_output, data_dir)

    path_log_for_this_run = os.path.join(path_logs,
                                         f'logs_{data_dir_name}_{datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.log')

    # create the output dir and the log files
    if not os.path.exists(path_output):
        os.mkdir(path_output)
    if not os.path.exists(path_output_data):
        os.mkdir(path_output_data)

    # print the run config data
    logger.info(f"Current script's dir: {os.path.dirname(os.path.abspath(__file__))}")
    logger.info(f"\t- root_dir: {get_project_root()}")
    logger.info(f"\t- home_dir: {home_dir}")
    logger.info(f"\t- path_output: {path_output}")
    logger.info(f"\t- path_output_data: {path_output_data}")
    logger.info(f"\t- path_logs: {path_log_for_this_run}")
    logger.info(f"\t- path_browser: {path_browser}")
    logger.info(f'\t- path_db: {os.path.join(get_project_root(), "db/accounts.db")}')
    if seed_query:
        logger.info(f"Seed query for this run: {seed_query}")
    else:
        logger.info(f"Query is a custom query")
    logger.info(f"Limits: account ({lim_acc}), browser ({lim_browser})")
    if start_date and end_date and date_ranges:
        logger.info(f"Dates from {start_date} --> {end_date} [{len(date_ranges)} date ranges]")
    logger.info(f"Use case: {use_case}")
    logger.info(f"Scrape method: {scrape_method}")

    scraper = TwitterScraper(
        lim_acc=lim_acc,
        lim_browser=lim_browser,
        path_db=os.path.join(os.path.join(get_project_root(), "db", "accounts.db")),
        headless=True,
        use_case=use_case,
        path_browser=path_browser
    )

    # get custom or generated queries
    # validate with pydantic model
    if seed_query:
        queries: list[SearchQuery | TimelineQuery] = generate_queries(
            seed_query=seed_query,
            scrape_method=scrape_method,
            home_dir=home_dir,
            path_output_data=path_output_data,
            start_date=start_date,
            end_date=end_date,
            date_ranges=date_ranges
        )
    else:
        queries: list[SearchQuery | TimelineQuery | ExploreQuery | SearchExploreQuery] = []
        for q in config.custom_queries:
            if scrape_method == "search":
                queries.append(
                    SearchQuery(
                        query=q.query,
                        path=os.path.join(path_output_data, q.filename),
                        seed_info=q.seed_info,
                        start_date=q.start_date,
                        end_date=q.end_date,
                        update_phh_history=q.update_phh_history,
                        bool_upload_to_s3=q.bool_upload_to_s3,
                        bool_change_to_new_format=q.bool_change_to_new_format,
                        force_collection=q.force_collection
                    )
                )
            elif scrape_method == "explore":
                queries.append(
                    ExploreQuery(
                        query=q.query,
                        path=os.path.join(path_output_data, q.filename),
                        bool_upload_to_s3=q.bool_upload_to_s3
                    )
                )
            elif scrape_method == 'search_explore':
                queries.append(
                    SearchExploreQuery(
                        query=q.query,
                        path=os.path.join(path_output_data, q.filename),
                        bool_upload_to_s3=q.bool_upload_to_s3
                    )
                )
            else:  # scrape_method is timeline
                queries.append(
                    TimelineQuery(
                        query=int(q.query),
                        path=os.path.join(path_output_data, q.filename),
                        seed_info=q.seed_info,
                        start_date=q.start_date,
                        end_date=q.end_date,
                        stopping_condition=lambda x: date_stopping_condition(x, start_date),
                        update_phh_history=True,
                        bool_upload_to_s3=True,
                        bool_change_to_new_format=True,
                        force_collection=False
                    )
                )
    # filter out the queries
    queries: list[dict] = [q.to_dict() for q in queries]
    queries: list[dict] = [i for i in queries if not os.path.exists(i["path"])]
    if config.limit.queries > 0:
        queries: list[dict] = random.sample(queries, config.limit.queries)

    if scrape_method == "timeline":
        scrape_meta_data: dict = asyncio.run(
            scraper.user_tweets_scrape(
                queries=queries
            )
        )
    elif scrape_method == "search":
        scrape_meta_data: dict = asyncio.run(
            scraper.search_queries_scrape(
                queries=queries
            )
        )
    elif scrape_method == "explore":
        scrape_meta_data: dict = asyncio.run(
            scraper.explore_scrape(
                queries=queries
            )
        )
    elif scrape_method == "search_explore":
        scrape_meta_data: dict = asyncio.run(
            scraper.search_explore_scrape(
                queries=queries
            )
        )
    else:
        raise ValueError(f"Scrape method must either be timeline or search !")
    results: Optional[list[dict]] = None
    if scrape_method == "explore":
        results = scrape_meta_data["scraping_results"]
        scrape_meta_data: dict = scrape_meta_data["meta_data"]
    # show meta-data
    total_tweets_collected: int = scrape_meta_data["total_tweets_collected"]
    total_scraped_queries: int = scrape_meta_data["total_num_queries"]
    start_time: str = scrape_meta_data["start_time"]
    end_time: str = scrape_meta_data["end_time"]
    post_scraping_accounts: dict = scrape_meta_data["post_scraping_accounts"]
    logger.info(f"Total tweets collected: {total_tweets_collected}")
    logger.info(f"Total scraped queries: {total_scraped_queries}")
    logger.info(f"Start time: {start_time}")
    logger.info(f"End time: {end_time}")
    logger.info(f"Post scraping accounts:")
    msg: str = "".join(f"\n\t- {username}: active {active}" for username, active in post_scraping_accounts.items())
    logger.info(msg)
    if scrape_method == "explore":
        return {"scrape_meta_data": scrape_meta_data, "scraping_results": results}
    return scrape_meta_data


def collect_trends(scrape_meta_data: dict) -> dict:
    job_id: str = scrape_meta_data["job_id"]
    query: str = scrape_meta_data["query"]  # change what the query is in API
    now: datetime.datetime = datetime.datetime.now().replace(tzinfo=tz.gettz('US/Eastern'))

    # setup logging
    setup_logging(job_id=job_id)
    logger = get_logger()
    logger.info(f"Getting Twitter trending for {now.strftime('%Y-%m-%d')} (US/Eastern)")

    try:
        config: dict = {
            "use_case": 1,
            "s3paths": {
                "bucket": "meo-raw-data",
                "folder": "keywords-based-data/twitter/explore_page",
                "meta_folder": "twitter/meta"
            },
            "dates": {"start_date": now.strftime('%Y-%m-%d'), "end_date": now.strftime('%Y-%m-%d')},
            "custom_queries_dirname": f"{query}_keywords_{now.strftime('%Y-%m-%d_%H_%M_%S')}",
            # .strftime("%Y-%m-%d %H:%M:%S")
            "scrape_method": f"explore",
            "custom_queries": [
                {"query": f"{query}",
                 "filename": f"explore_{query}_keywords_{now.strftime('%Y-%m-%d_%H_%M_%S')}.jsonl",
                 "start_date": now.strftime('%Y-%m-%d'),
                 }
            ]
        }

        config: ConfigModel = ConfigModel(**config)

        # .env
        load_dotenv()

        # AWS access setup
        AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
        os.environ["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
        AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
        os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY
        AWS_REGION = os.getenv("AWS_REGION")
        os.environ["AWS_REGION"] = AWS_REGION

        # make bucket environmental
        os.environ["S3BUCKET"] = config.s3paths.bucket  # config["s3paths"]["bucket"]
        os.environ["S3FOLDER"] = config.s3paths.folder  # config["s3paths"]["folder"]
        os.environ["S3META_FOLDER"] = config.s3paths.meta_folder  # config["s3paths"]["meta-folder"]

        return scrape_twitter_(config=config)

    except Exception as e:
        logger.error(f'Scraping job failed with exception {e}')
        logger.error(traceback.format_exc())
        return {"status": "failed", "error": str(e), "job_id": job_id}


def collect_trending_tweets(scrape_meta_data: dict) -> dict:
    def make_keyword_file_compatible(keyword: str) -> str:
        keyword = keyword.replace(" ", "_")
        keyword = keyword.lower()
        keyword = keyword.replace("#", "")
        keyword = keyword.replace("-", "_")
        return keyword

    job_id: str = scrape_meta_data["job_id"]
    query: str = scrape_meta_data["query"]
    keywords: list[str] | str = scrape_meta_data["keywords"]
    now: datetime.datetime = datetime.datetime.now().replace(tzinfo=tz.gettz('US/Eastern'))

    # setup logging
    setup_logging(job_id=job_id)
    logger = get_logger()
    logger.info(f"Getting Twitter {query} and their tweets for {now.strftime('%Y-%m-%d')} (US/Eastern)")

    try:
        keywords = keywords if isinstance(keywords, list) else [keywords]
        config: dict = {
            "use_case": 1,
            "s3paths": {
                "bucket": "meo-raw-data",
                "folder": "keywords-based-data/twitter/keywords",
                "meta_folder": "twitter/meta"
            },
            "dates": {"start_date": now.strftime('%Y-%m-%d'), "end_date": now.strftime('%Y-%m-%d')},
            "custom_queries_dirname": f"{query}_tweets_{now.strftime('%Y-%m-%d_%H_%M_%S')}",
            "scrape_method": f"search_explore",
            "custom_queries": [
                {"query": f"{k}",
                 "filename": f"{make_keyword_file_compatible(k)}_{query}_keywords_{now.strftime('%Y-%m-%d_%H_%M_%S')}.jsonl"
                 } for k in keywords
            ]
        }

        config: ConfigModel = ConfigModel(**config)

        # .env
        load_dotenv()

        # AWS access setup
        AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
        os.environ["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
        AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
        os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY
        AWS_REGION = os.getenv("AWS_REGION")
        os.environ["AWS_REGION"] = AWS_REGION

        # make bucket environmental
        os.environ["S3BUCKET"] = config.s3paths.bucket  # config["s3paths"]["bucket"]
        os.environ["S3FOLDER"] = config.s3paths.folder  # config["s3paths"]["folder"]
        os.environ["S3META_FOLDER"] = config.s3paths.meta_folder  # config["s3paths"]["meta-folder"]

        return scrape_twitter_(config=config)

    except Exception as e:
        logger.error(f'Scraping job failed with exception {e}')
        logger.error(traceback.format_exc())
        return {"status": "failed", "error": str(e), "job_id": job_id}


def collect_trends_and_tweets(scrape_meta_data: dict) -> dict:
    job_id: str = scrape_meta_data["job_id"]
    query: str = scrape_meta_data["query"]  # change what the query is in API
    now: datetime.datetime = datetime.datetime.now().replace(tzinfo=tz.gettz('US/Eastern'))

    # setup logging
    setup_logging(job_id=job_id)
    logger = get_logger()
    logger.info(f"Getting Twitter explore page {query} for {now.strftime('%Y-%m-%d')} (US/Eastern) with job_id {job_id}")

    try:
        # scrape trends
        trends_scrape_dict: dict = collect_trends(
            {"job_id": job_id,
             "query": query}
        )  # {"scrape_meta_data": scrape_meta_data, "scraping_results": results}

        # check failed -  scraping what's trending
        if "status" in trends_scrape_dict and trends_scrape_dict["status"] == "failed":
            raise Exception(f"Exception when scraping {query} for job {job_id} and query {query}"
                            f"\n\t - status: {trends_scrape_dict['status']}"
                            f"\n\t - error: {trends_scrape_dict['error']}")

        trends_meta_data: dict = trends_scrape_dict["scrape_meta_data"]
        trends_scraping_results: list = trends_scrape_dict["scraping_results"]  # list of TimelineTrends
        trends_keywords: list[str] = [t["name"] for t in trends_scraping_results]  # extract the names of scraped trends

        # scrape tweets in trend
        trends_tweets_scrape: dict = collect_trending_tweets(
            {"job_id": job_id, "query": query,
             "keywords": trends_keywords}
        )  # just meta_data dict

        # check failed - tweets in trending
        if "status" in trends_tweets_scrape:
            raise Exception(f"Exception when scraping for {query} tweets for job {job_id} and query {query}"
                            f"\n\t - status: {trends_tweets_scrape['status']}"
                            f"\n\t - error: {trends_tweets_scrape['error']}")
        return {
            "collect_trends": {
                "trends_meta_data": trends_meta_data,
                "trends_scraping_results": trends_scraping_results
            },
            "collect_trending_tweets": trends_tweets_scrape
        }

    except Exception as e:
        logger.error(f'Scraping job failed with exception {e}')
        logger.error(traceback.format_exc())
        return {"status": "failed", "error": str(e), "job_id": job_id}


def run_scraper_daily(scrape_meta_data: dict):
    job_id: str = scrape_meta_data["job_id"]
    query: str = scrape_meta_data["query"]  # change what the query is in API
    now: datetime.datetime = datetime.datetime.now()
    start_date = (now - datetime.timedelta(days=4)).strftime("%Y-%m-%d")
    end_date = (now - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    # setup logging
    setup_logging(job_id=job_id)
    logger = get_logger()
    logger.info(f"Starting daily scrape from {start_date} to {end_date} with scrape_meta_data {scrape_meta_data}")

    try:
        # TODO: create a config model for each type of scrape model
        config: dict = {
            "dates": {"start_date": start_date, "end_date": end_date},
            "limit": {"accounts": 25, "browsers": 6},
            "seed_query": query,
            "use_case": 0,
            "scrape_method": "timeline"
        }
        config: ConfigModel = ConfigModel(**config)

        # .env
        load_dotenv()

        # AWS access setup
        AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
        os.environ["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
        AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
        os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY
        AWS_REGION = os.getenv("AWS_REGION")
        os.environ["AWS_REGION"] = AWS_REGION

        # make bucket environmental
        os.environ["S3BUCKET"] = config.s3paths.bucket  # config["s3paths"]["bucket"]
        os.environ["S3FOLDER"] = config.s3paths.folder  # config["s3paths"]["folder"]
        os.environ["S3META_FOLDER"] = config.s3paths.meta_folder  # config["s3paths"]["meta-folder"]

        return scrape_twitter_(config=config)
    except Exception as e:
        logger.error(f'Scraping job failed with exception {e}')
        logger.error(traceback.format_exc())
        return {"status": "failed", "error": str(e), "job_id": job_id}


def run_scraper_tridaily(scrape_meta_data: dict):
    # TODO: create a config model for each type of scrape model
    job_id: str = scrape_meta_data["job_id"]
    query: str = scrape_meta_data["query"]  # change what the query is in API
    now: datetime.datetime = datetime.datetime.now()
    start_date = (now - datetime.timedelta(days=6)).strftime("%Y-%m-%d")
    end_date = (now - datetime.timedelta(days=3)).strftime("%Y-%m-%d")  # scrape for the past three days

    # setup logging
    setup_logging(job_id=job_id)
    logger = get_logger()
    logger.info(f"Starting tridaily scrape from {start_date} to {end_date} with scrape_meta_data: {scrape_meta_data}")
    try:
        config: dict = {
            "dates": {"start_date": start_date, "end_date": end_date},
            "limit": {"accounts": 25, "browsers": 6},
            "seed_query": query,
            "use_case": 1,
            "scrape_method": "search"
        }
        config: ConfigModel = ConfigModel(**config)

        # .env
        load_dotenv()

        # AWS access setup
        AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
        os.environ["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
        AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
        os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY
        AWS_REGION = os.getenv("AWS_REGION")
        os.environ["AWS_REGION"] = AWS_REGION

        # make bucket environmental
        os.environ["S3BUCKET"] = config.s3paths.bucket  # config["s3paths"]["bucket"]
        os.environ["S3FOLDER"] = config.s3paths.folder  # config["s3paths"]["folder"]
        os.environ["S3META_FOLDER"] = config.s3paths.meta_folder  # config["s3paths"]["meta-folder"]

        return scrape_twitter_(config=config)
    except Exception as e:
        logger.error(f'Scraping job failed with exception {e}')
        logger.error(traceback.format_exc())
        return {"status": "failed", "error": str(e), "job_id": job_id}


def run_scraper(jobs_meta_data: dict):
    config: ConfigModel = ConfigModel(**jobs_meta_data["config"])
    job_id: str | None = jobs_meta_data["job_id"]

    try:
        # setup logging - filepath and all
        setup_logging(_log_level=config._log_level.upper(), job_id=job_id)
        logger = get_logger()
        logger.info("Starting scraping !")

        # .env
        load_dotenv()

        # AWS access setup
        AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
        os.environ["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
        AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
        os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY
        AWS_REGION = os.getenv("AWS_REGION")
        os.environ["AWS_REGION"] = AWS_REGION

        # make bucket environmental
        os.environ["S3BUCKET"] = config.s3paths.bucket  # config["s3paths"]["bucket"]
        os.environ["S3FOLDER"] = config.s3paths.folder  # config["s3paths"]["folder"]
        os.environ["S3META_FOLDER"] = config.s3paths.meta_folder  # config["s3paths"]["meta-folder"]

        return scrape_twitter_(config=config)
    except Exception as e:
        if job_id:
            setup_logging(job_id)
            logger = get_logger()
            logger.error(f'Scraping job failed with exception {e}')
            logger.error(traceback.format_exc())
        return {"status": "failed", "error": str(e), "job_id": job_id}


# create lifespan event
def enqueue_front_with_unique_id(scraping_func: str, queue: str, **kwargs):
    """Enqueue job with unique id"""
    kwargs_str = None
    try:
        query = kwargs["query"]
        job_id = str(uuid.uuid4())
        job_metadata = {
            "timestamp (utc)": datetime.datetime.utcnow().isoformat(),
            "timestamp (US/Eastern)": datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).astimezone(
                tz=tz.gettz('US/Eastern')).isoformat(),
            "job_id": job_id,
            "query": query
        }
        if queue == "scraper_queue":
            queue: Queue = scraper_queue
        elif queue == "accounts_queue":
            queue: Queue = account_queue
        else:
            raise Exception("Unknown queue")

        for k, v in kwargs.items():  # add job kwargs
            job_metadata[k] = v

        job = queue.create_job(
            scraping_func,
            args=(job_metadata,),
            timeout="12h",
            result_ttl=604800,  # keep for 7 days
            failure_ttl=604800,  # keep for 7 days
            job_id=job_id,
            meta=job_metadata,
            retry=Retry(max=3, interval=60)
        )
        # push to the front
        queue.enqueue_job(job, at_front=True)
        try:
            kwargs_str = json.dumps(kwargs)
        except Exception as e:
            pass
        logger.info(f"Job {scraping_func} enqueued at front successfully with job_metadata {job_metadata}")
        return {"success": True,
                "message": f"Job enqueued to front for \n\t* function name {scraping_func},"
                           f"\n\t*queue {queue}, "
                           f"\n\t*kwargs {kwargs_str}",
                "job_id": job_id
                }
    except Exception as e:
        logger.error(f"Failed to enqueue job: {str(e)}")
        return {"success": False,
                "message": f"Failed to enqueue job at front for \n\t* function name {scraping_func},"
                           f"\n\t*queue {queue}, "
                           f"\n\t*kwargs {kwargs_str}",
                "error": str(e)
                }


if __name__ == "__main__":
    config = {
        "dates": {"start_date": "2025-02-21", "end_date": "2025-02-22"},
        # "paths": {"output": "/Users/mikad/MEOMcGill/meo_twitter_scraper/twitter-crawler/output"},
        "limit": {"accounts": 25, "browsers": 6},
        "seed_query": "Platform:twitter AND Handle:NDPJulia",
        # "Platform:twitter AND (MainType:news_outlet OR SubType:media)",
        "use_case": 2,
        "scrape_method": "timeline"
    }

    # scrape trends
    # trends_scrape_dict: dict = collect_trends({"job_id": None, "query": "trending"}) # {"scrape_meta_data": scrape_meta_data, "scraping_results": results}
    # trends: list = trends_scrape_dict["scraping_results"]
    # trends = [{'id': 'trend-Gene Hackman', 'rank': 1, 'name': 'Gene Hackman', 'trend_url': {'url': 'twitter://search/?query=%22Gene+Hackman%22&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgMR2VuZSBIYWNrbWFuAAA='}]}}, 'trend_metadata': {'domain_context': 'Trending in Canada', 'meta_description': '232K posts', 'url': {'url': 'twitter://search/?query=%22Gene+Hackman%22&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgMR2VuZSBIYWNrbWFuAAA='}]}}}, 'grouped_trends': [{'name': 'Mississippi Burning', 'url': {'url': 'twitter://search/?query=Mississippi+Burning&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgTTWlzc2lzc2lwcGkgQnVybmluZwAA'}]}}}, {'name': 'The French Connection', 'url': {'url': 'twitter://search/?query=The+French+Connection&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgVVGhlIEZyZW5jaCBDb25uZWN0aW9uAAA='}]}}}, {'name': 'The Conversation', 'url': {'url': 'twitter://search/?query=The+Conversation&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgQVGhlIENvbnZlcnNhdGlvbgAA'}]}}}, {'name': 'The Birdcage', 'url': {'url': 'twitter://search/?query=The+Birdcage&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgMVGhlIEJpcmRjYWdlAAA='}]}}}], 'date': '2025-02-27 12:23:20', '_type': 'timelinetrend'}, {'id': 'trend-#Flames1stGoal', 'rank': 2, 'name': '#Flames1stGoal', 'trend_url': {'url': 'twitter://search/?query=%23Flames1stGoal&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgOI0ZsYW1lczFzdEdvYWwAAA=='}]}}, 'trend_metadata': {'domain_context': 'Trending in Canada', 'url': {'url': 'twitter://search/?query=%23Flames1stGoal&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgOI0ZsYW1lczFzdEdvYWwAAA=='}]}}}, 'grouped_trends': None, 'date': '2025-02-27 12:23:20', '_type': 'timelinetrend'}, {'id': 'trend-Epstein', 'rank': 3, 'name': 'Epstein', 'trend_url': {'url': 'twitter://search/?query=Epstein&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgHRXBzdGVpbgAA'}]}}, 'trend_metadata': {'domain_context': 'Trending in Canada', 'meta_description': '753K posts', 'url': {'url': 'twitter://search/?query=Epstein&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgHRXBzdGVpbgAA'}]}}}, 'grouped_trends': None, 'date': '2025-02-27 12:23:20', '_type': 'timelinetrend'}, {'id': 'trend-Shrek', 'rank': 4, 'name': 'Shrek', 'trend_url': {'url': 'twitter://search/?query=Shrek&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgFU2hyZWsAAA=='}]}}, 'trend_metadata': {'domain_context': 'Trending in Canada', 'meta_description': '109K posts', 'url': {'url': 'twitter://search/?query=Shrek&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgFU2hyZWsAAA=='}]}}}, 'grouped_trends': [{'name': 'Zendaya', 'url': {'url': 'twitter://search/?query=Zendaya&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgHWmVuZGF5YQAA'}]}}}], 'date': '2025-02-27 12:23:20', '_type': 'timelinetrend'}, {'id': 'trend-#PinkShirtDay', 'rank': 5, 'name': '#PinkShirtDay', 'trend_url': {'url': 'twitter://search/?query=%23PinkShirtDay&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgNI1BpbmtTaGlydERheQAA'}]}}, 'trend_metadata': {'domain_context': 'Trending in Canada', 'meta_description': '1,780 posts', 'url': {'url': 'twitter://search/?query=%23PinkShirtDay&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgNI1BpbmtTaGlydERheQAA'}]}}}, 'grouped_trends': None, 'date': '2025-02-27 12:23:20', '_type': 'timelinetrend'}, {'id': 'trend-Tate', 'rank': 6, 'name': 'Tate', 'trend_url': {'url': 'twitter://search/?query=Tate&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgEVGF0ZQAA'}]}}, 'trend_metadata': {'domain_context': 'Trending in Canada', 'meta_description': '147K posts', 'url': {'url': 'twitter://search/?query=Tate&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgEVGF0ZQAA'}]}}}, 'grouped_trends': [{'name': 'Romania', 'url': {'url': 'twitter://search/?query=Romania&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgHUm9tYW5pYQAA'}]}}}, {'name': 'Crimson Tide', 'url': {'url': 'twitter://search/?query=Crimson+Tide&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgMQ3JpbXNvbiBUaWRlAAA='}]}}}], 'date': '2025-02-27 12:23:20', '_type': 'timelinetrend'}, {'id': 'trend-#OntarioVotes2025', 'rank': 7, 'name': '#OntarioVotes2025', 'trend_url': {'url': 'twitter://search/?query=%23OntarioVotes2025&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgRI09udGFyaW9Wb3RlczIwMjUAAA=='}]}}, 'trend_metadata': {'domain_context': 'Trending in Canada', 'meta_description': '2,128 posts', 'url': {'url': 'twitter://search/?query=%23OntarioVotes2025&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgRI09udGFyaW9Wb3RlczIwMjUAAA=='}]}}}, 'grouped_trends': None, 'date': '2025-02-27 12:23:20', '_type': 'timelinetrend'}, {'id': 'trend-Chikorita', 'rank': 8, 'name': 'Chikorita', 'trend_url': {'url': 'twitter://search/?query=Chikorita&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgJQ2hpa29yaXRhAAA='}]}}, 'trend_metadata': {'domain_context': 'Trending in Canada', 'meta_description': '27.5K posts', 'url': {'url': 'twitter://search/?query=Chikorita&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgJQ2hpa29yaXRhAAA='}]}}}, 'grouped_trends': [{'name': 'Totodile', 'url': {'url': 'twitter://search/?query=Totodile&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgIVG90b2RpbGUAAA=='}]}}}, {'name': '#PokemonDay', 'url': {'url': 'twitter://search/?query=%23PokemonDay&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgLI1Bva2Vtb25EYXkAAA=='}]}}}, {'name': 'Tepig', 'url': {'url': 'twitter://search/?query=Tepig&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgFVGVwaWcAAA=='}]}}}, {'name': 'Scarlet and Violet', 'url': {'url': 'twitter://search/?query=Scarlet+and+Violet&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgSU2NhcmxldCBhbmQgVmlvbGV0AAA='}]}}}], 'date': '2025-02-27 12:23:20', '_type': 'timelinetrend'}, {'id': 'trend-#5YearsofTheManMusicVideo', 'rank': 9, 'name': '#5YearsofTheManMusicVideo', 'trend_url': {'url': 'twitter://search/?query=%235YearsofTheManMusicVideo&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgZIzVZZWFyc29mVGhlTWFuTXVzaWNWaWRlbwAA'}]}}, 'trend_metadata': {'domain_context': 'Trending in Canada', 'meta_description': '3,782 posts', 'url': {'url': 'twitter://search/?query=%235YearsofTheManMusicVideo&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgZIzVZZWFyc29mVGhlTWFuTXVzaWNWaWRlbwAA'}]}}}, 'grouped_trends': None, 'date': '2025-02-27 12:23:20', '_type': 'timelinetrend'}, {'id': 'trend-Carney', 'rank': 10, 'name': 'Carney', 'trend_url': {'url': 'twitter://search/?query=Carney&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgGQ2FybmV5AAA='}]}}, 'trend_metadata': {'domain_context': 'Trending in Canada', 'meta_description': '113K posts', 'url': {'url': 'twitter://search/?query=Carney&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgGQ2FybmV5AAA='}]}}}, 'grouped_trends': None, 'date': '2025-02-27 12:23:20', '_type': 'timelinetrend'}, {'id': 'trend-Éric Caire', 'rank': 11, 'name': 'Éric Caire', 'trend_url': {'url': 'twitter://search/?query=%22%C3%89ric+Caire%22&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgLw4lyaWMgQ2FpcmUAAA=='}]}}, 'trend_metadata': {'domain_context': 'Trending in Canada', 'meta_description': '2,631 posts', 'url': {'url': 'twitter://search/?query=%22%C3%89ric+Caire%22&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgLw4lyaWMgQ2FpcmUAAA=='}]}}}, 'grouped_trends': [{'name': 'SAAQclic', 'url': {'url': 'twitter://search/?query=SAAQclic&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgIU0FBUWNsaWMAAA=='}]}}}], 'date': '2025-02-27 12:23:20', '_type': 'timelinetrend'}, {'id': 'trend-Brookfield', 'rank': 12, 'name': 'Brookfield', 'trend_url': {'url': 'twitter://search/?query=Brookfield&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgKQnJvb2tmaWVsZAAA'}]}}, 'trend_metadata': {'domain_context': 'Trending in Canada', 'meta_description': '24.5K posts', 'url': {'url': 'twitter://search/?query=Brookfield&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgKQnJvb2tmaWVsZAAA'}]}}}, 'grouped_trends': None, 'date': '2025-02-27 12:23:20', '_type': 'timelinetrend'}, {'id': 'trend-sabrina', 'rank': 13, 'name': 'sabrina', 'trend_url': {'url': 'twitter://search/?query=sabrina&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgHc2FicmluYQAA'}]}}, 'trend_metadata': {'domain_context': 'Trending in Canada', 'meta_description': '57.7K posts', 'url': {'url': 'twitter://search/?query=sabrina&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgHc2FicmluYQAA'}]}}}, 'grouped_trends': None, 'date': '2025-02-27 12:23:20', '_type': 'timelinetrend'}, {'id': 'trend-Megas', 'rank': 14, 'name': 'Megas', 'trend_url': {'url': 'twitter://search/?query=Megas&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgFTWVnYXMAAA=='}]}}, 'trend_metadata': {'domain_context': 'Trending in Canada', 'meta_description': '7,299 posts', 'url': {'url': 'twitter://search/?query=Megas&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgFTWVnYXMAAA=='}]}}}, 'grouped_trends': None, 'date': '2025-02-27 12:23:20', '_type': 'timelinetrend'}, {'id': 'trend-Michelle Trachtenberg', 'rank': 15, 'name': 'Michelle Trachtenberg', 'trend_url': {'url': 'twitter://search/?query=%22Michelle+Trachtenberg%22&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgVTWljaGVsbGUgVHJhY2h0ZW5iZXJnAAA='}]}}, 'trend_metadata': {'domain_context': 'Trending in Canada', 'meta_description': '497K posts', 'url': {'url': 'twitter://search/?query=%22Michelle+Trachtenberg%22&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgVTWljaGVsbGUgVHJhY2h0ZW5iZXJnAAA='}]}}}, 'grouped_trends': None, 'date': '2025-02-27 12:23:20', '_type': 'timelinetrend'}, {'id': 'trend-Jagmeet', 'rank': 16, 'name': 'Jagmeet', 'trend_url': {'url': 'twitter://search/?query=Jagmeet&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgHSmFnbWVldAAA'}]}}, 'trend_metadata': {'domain_context': 'Trending in Canada', 'meta_description': '23.6K posts', 'url': {'url': 'twitter://search/?query=Jagmeet&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgHSmFnbWVldAAA'}]}}}, 'grouped_trends': None, 'date': '2025-02-27 12:23:20', '_type': 'timelinetrend'}, {'id': 'trend-#Survivor48', 'rank': 17, 'name': '#Survivor48', 'trend_url': {'url': 'twitter://search/?query=%23Survivor48&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgLI1N1cnZpdm9yNDgAAA=='}]}}, 'trend_metadata': {'domain_context': 'Trending in Canada', 'meta_description': '9,087 posts', 'url': {'url': 'twitter://search/?query=%23Survivor48&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgLI1N1cnZpdm9yNDgAAA=='}]}}}, 'grouped_trends': None, 'date': '2025-02-27 12:23:20', '_type': 'timelinetrend'}, {'id': 'trend-Centel', 'rank': 18, 'name': 'Centel', 'trend_url': {'url': 'twitter://search/?query=Centel&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgGQ2VudGVsAAA='}]}}, 'trend_metadata': {'domain_context': 'Trending in Canada', 'meta_description': '106K posts', 'url': {'url': 'twitter://search/?query=Centel&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgGQ2VudGVsAAA='}]}}}, 'grouped_trends': None, 'date': '2025-02-27 12:23:20', '_type': 'timelinetrend'}, {'id': 'trend-Savoie', 'rank': 19, 'name': 'Savoie', 'trend_url': {'url': 'twitter://search/?query=Savoie&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgGU2F2b2llAAA='}]}}, 'trend_metadata': {'domain_context': 'Trending in Canada', 'url': {'url': 'twitter://search/?query=Savoie&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgGU2F2b2llAAA='}]}}}, 'grouped_trends': None, 'date': '2025-02-27 12:23:20', '_type': 'timelinetrend'}, {'id': 'trend-Johto', 'rank': 20, 'name': 'Johto', 'trend_url': {'url': 'twitter://search/?query=Johto&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgFSm9odG8AAA=='}]}}, 'trend_metadata': {'domain_context': 'Trending in Canada', 'meta_description': '8,663 posts', 'url': {'url': 'twitter://search/?query=Johto&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgFSm9odG8AAA=='}]}}}, 'grouped_trends': None, 'date': '2025-02-27 12:23:20', '_type': 'timelinetrend'}, {'id': 'trend-DAM MV TEASER', 'rank': 21, 'name': 'DAM MV TEASER', 'trend_url': {'url': 'twitter://search/?query=%22DAM+MV+TEASER%22&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgNREFNIE1WIFRFQVNFUgAA'}]}}, 'trend_metadata': {'domain_context': 'Trending in Canada', 'meta_description': '202K posts', 'url': {'url': 'twitter://search/?query=%22DAM+MV+TEASER%22&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgNREFNIE1WIFRFQVNFUgAA'}]}}}, 'grouped_trends': None, 'date': '2025-02-27 12:23:20', '_type': 'timelinetrend'}, {'id': 'trend-Carbon Monoxide', 'rank': 22, 'name': 'Carbon Monoxide', 'trend_url': {'url': 'twitter://search/?query=%22Carbon+Monoxide%22&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgPQ2FyYm9uIE1vbm94aWRlAAA='}]}}, 'trend_metadata': {'domain_context': 'Trending in Canada', 'meta_description': '23.4K posts', 'url': {'url': 'twitter://search/?query=%22Carbon+Monoxide%22&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgPQ2FyYm9uIE1vbm94aWRlAAA='}]}}}, 'grouped_trends': None, 'date': '2025-02-27 12:23:20', '_type': 'timelinetrend'}, {'id': 'trend-Onana', 'rank': 23, 'name': 'Onana', 'trend_url': {'url': 'twitter://search/?query=Onana&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgFT25hbmEAAA=='}]}}, 'trend_metadata': {'domain_context': 'Trending in Canada', 'meta_description': '75.6K posts', 'url': {'url': 'twitter://search/?query=Onana&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgFT25hbmEAAA=='}]}}}, 'grouped_trends': None, 'date': '2025-02-27 12:23:20', '_type': 'timelinetrend'}, {'id': 'trend-Burna', 'rank': 24, 'name': 'Burna', 'trend_url': {'url': 'twitter://search/?query=Burna&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgFQnVybmEAAA=='}]}}, 'trend_metadata': {'domain_context': 'Trending in Canada', 'meta_description': '33.4K posts', 'url': {'url': 'twitter://search/?query=Burna&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgFQnVybmEAAA=='}]}}}, 'grouped_trends': [{'name': 'Lambo', 'url': {'url': 'twitter://search/?query=Lambo&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgFTGFtYm8AAA=='}]}}}], 'date': '2025-02-27 12:23:20', '_type': 'timelinetrend'}, {'id': 'trend-Ebola', 'rank': 25, 'name': 'Ebola', 'trend_url': {'url': 'twitter://search/?query=Ebola&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgFRWJvbGEAAA=='}]}}, 'trend_metadata': {'domain_context': 'Trending in Canada', 'meta_description': '73.9K posts', 'url': {'url': 'twitter://search/?query=Ebola&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgFRWJvbGEAAA=='}]}}}, 'grouped_trends': None, 'date': '2025-02-27 12:23:20', '_type': 'timelinetrend'}, {'id': 'trend-New Mexico', 'rank': 26, 'name': 'New Mexico', 'trend_url': {'url': 'twitter://search/?query=%22New+Mexico%22&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgKTmV3IE1leGljbwAA'}]}}, 'trend_metadata': {'domain_context': 'Trending in Canada', 'meta_description': '40.1K posts', 'url': {'url': 'twitter://search/?query=%22New+Mexico%22&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgKTmV3IE1leGljbwAA'}]}}}, 'grouped_trends': None, 'date': '2025-02-27 12:23:20', '_type': 'timelinetrend'}, {'id': 'trend-Gen 2', 'rank': 27, 'name': 'Gen 2', 'trend_url': {'url': 'twitter://search/?query=%22Gen+2%22&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgFR2VuIDIAAA=='}]}}, 'trend_metadata': {'domain_context': 'Trending in Canada', 'meta_description': '8,781 posts', 'url': {'url': 'twitter://search/?query=%22Gen+2%22&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgFR2VuIDIAAA=='}]}}}, 'grouped_trends': None, 'date': '2025-02-27 12:23:20', '_type': 'timelinetrend'}, {'id': 'trend-DeSantis', 'rank': 28, 'name': 'DeSantis', 'trend_url': {'url': 'twitter://search/?query=DeSantis&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgIRGVTYW50aXMAAA=='}]}}, 'trend_metadata': {'domain_context': 'Trending in Canada', 'meta_description': '22.1K posts', 'url': {'url': 'twitter://search/?query=DeSantis&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgIRGVTYW50aXMAAA=='}]}}}, 'grouped_trends': None, 'date': '2025-02-27 12:23:20', '_type': 'timelinetrend'}, {'id': 'trend-Gretzky', 'rank': 29, 'name': 'Gretzky', 'trend_url': {'url': 'twitter://search/?query=Gretzky&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgHR3JldHpreQAA'}]}}, 'trend_metadata': {'domain_context': 'Trending in Canada', 'meta_description': '10.9K posts', 'url': {'url': 'twitter://search/?query=Gretzky&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgHR3JldHpreQAA'}]}}}, 'grouped_trends': None, 'date': '2025-02-27 12:23:20', '_type': 'timelinetrend'}, {'id': 'trend-Cyndaquil', 'rank': 30, 'name': 'Cyndaquil', 'trend_url': {'url': 'twitter://search/?query=Cyndaquil&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgJQ3luZGFxdWlsAAA='}]}}, 'trend_metadata': {'domain_context': 'Trending in Canada', 'meta_description': '3,869 posts', 'url': {'url': 'twitter://search/?query=Cyndaquil&src=trend_click&pc=true&vertical=trends', 'urlType': 'DeepLink', 'urtEndpointOptions': {'requestParams': [{'key': 'cd', 'value': 'HBgJQ3luZGFxdWlsAAA='}]}}}, 'grouped_trends': None, 'date': '2025-02-27 12:23:20', '_type': 'timelinetrend'}]
    # trends = [t["name"] for t in trends]
    # scrape tweets in trend
    # trends_tweets_scrape: dict = collect_trending_tweets({"job_id": None, "query": "trending", "keywords": trends})
    #res = collect_trends_and_tweets({"job_id": None, "query": "trending"})
    pass
    """# THE WORKER SCRIPT
    run_scraper(
        {
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "config": config,
            "job_id": None
            # "job_id": str(uuid.uuid4())
        }
    )"""
    run_scraper_daily({"job_id": str(uuid.uuid4()), "query": "Platform:twitter AND NOT (MainType:news_outlet OR SubType:media)"})
