from TwitterScraper import TwitterScraper
from my_utils.meo_api import get_seeds
from my_utils.folder_manipulation import open_jsonl
from twscrape.models import Tweet, parse_tweets
from twscrape.logger import logger

import configparser
import sys
import asyncio
from dotenv import load_dotenv
from pathlib import Path
import datetime
import os
import json
import random


def bin_date_range(start_date: datetime.datetime,
                   end_date: datetime.datetime,
                   size: int = 5
                   ) -> list[datetime.datetime] | None:
    if end_date is None or start_date is None or end_date < start_date or end_date.year == 9999:
        return []
    # create bins
    first_day = datetime.datetime(start_date.year, 1, 1)
    all_five_day_bins = []
    next_day = first_day
    changed_year_flag = False
    while next_day < end_date:
        if (next_day.year != first_day.year) and (not changed_year_flag):
            next_day = datetime.datetime(start_date.year + 1, 1, 1)
            changed_year_flag = True
        all_five_day_bins.append(next_day)
        next_day += datetime.timedelta(days=size)
        if next_day > end_date:
            all_five_day_bins.append(end_date)

    binned_range = [start_date]
    for i in sorted(set(all_five_day_bins)):
        if start_date < i < end_date:
            binned_range.append(i)
        elif i > end_date:
            break
    binned_range.append(end_date)
    binned_range = sorted(list(set(binned_range)))
    return binned_range


def sort_seeds(handles: list[dict[str, str]], path_setup: str) -> list[dict[str, str]]:
    """
    Parameters
    ----------
    handles : list[dict[str, str]]
        The handles needed to be sorted
    path_setup : str
        Where to find the dict which has the seed ordering
    Returns
    -------
        list[dict[str, str]]
    """
    with open(os.path.join(path_setup, "account_id_tweets_per_day.json"), "r") as f:
        seed_tweets_per_day = json.load(f)
        # if the ID isn't in the tweets per day JSON then assume it's a killer
        return sorted(
            handles,
            key=lambda x: seed_tweets_per_day[str(x["SeedID"])]
            if str(x["SeedID"]) in seed_tweets_per_day.keys()
            else 0,
            reverse=True
        )


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


def historical_twitter_scrape(config: configparser):
    try:  # ~ home dir
        home_dir = config['paths']['home']
    except KeyError:
        home_dir = Path().absolute()

    try:  # twitter_crawler dir
        library_dir = config['paths']['twitter_crawler']
    except KeyError:
        library_dir = Path().absolute()
    try:  # output
        path_output = config['paths']['output']
    except KeyError:
        path_output = os.path.join(library_dir, "output")

    path_setup = os.path.join(library_dir, "setup")
    path_logs = os.path.join(path_output, 'logs')

    try:
        # dates
        start_date = config['date']['start']
        end_date = config['date']['end']
    except KeyError:
        raise KeyError("Need to provide start and end date !")

    start_date_dt = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end_date_dt = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    if (end_date_dt - start_date_dt) > datetime.timedelta(days=5):
        date_ranges: list[datetime.datetime] = sorted(bin_date_range(start_date_dt, end_date_dt), reverse=True)
        date_ranges: list[str] = [date_range.strftime('%Y-%m-%d') for date_range in date_ranges]
        date_ranges: list[tuple[str, str]] = [(date_ranges[c + 1], i) for c, i in enumerate(date_ranges) if
                                              c != len(date_ranges) - 1]
    else:
        date_ranges: list[tuple[str, str]] = [(start_date, end_date)]

    if not (datetime.datetime.strptime(start_date, '%Y-%m-%d') < datetime.datetime.strptime(end_date, '%Y-%m-%d')):
        raise ValueError("Start date < end date.")

    try:  # browser
        browser_path = config['paths']['browser']
    except KeyError:
        browser_path = None

    try:
        # lim account & browser
        lim_acc = int(config['limit']['accounts'])
        lim_browser = int(config['limit']['browsers'])
    except KeyError:
        lim_acc = 20
        lim_browser = 7

    try:
        query = config['query']['query']
    except KeyError:
        query = 'Platform:Twitter'

    print(os.path.dirname(os.path.abspath(__file__)))

    handles = get_seeds(
        username=os.getenv('MEO_USERNAME'),
        password=os.getenv('MEO_PASSWORD')
    )
    handles = sort_seeds(handles, path_setup)

    try:  # data
        data_dir = config['path']['data']
    except KeyError:
        data_dir = os.path.join('data', f'twitter_{start_date}_{end_date}')

    path_data = os.path.join(path_output, "data")
    path_output_data = os.path.join(path_output, data_dir)
    path_logs = os.path.join(path_output, 'logs')
    path_log_for_this_run = os.path.join(path_logs, f'logs_daily_scrape_{start_date}_{end_date}.txt')

    # create the output dir and the log files
    if not os.path.exists(path_output):
        os.mkdir(path_output)
    if not os.path.exists(path_data):
        os.mkdir(path_data)
    if not os.path.exists(path_output_data):
        os.mkdir(path_output_data)
    if not os.path.exists(path_logs):
        os.mkdir(path_logs)

    # scraping method
    try:
        scrape_method = config['scrape_type']['method']
    except KeyError:
        scrape_method = "search"

    print(f"\t- home_dir: {home_dir}")
    print(f"\t- library_dir: {library_dir}")
    print(f"\t- path_output: {path_output}")
    print(f"\t- path_output_data: {path_output_data}")
    print(f"\t- path_setup: {path_setup}")

    try:
        use_case = config['scrape_type']['use_case']
        if use_case == "None":
            use_case = None
        else:
            use_case = int(use_case)
    except KeyError:
        use_case = 2

    scraper = TwitterScraper(
        lim_acc=lim_acc,
        lim_browser=lim_browser,
        setup_path=path_setup,
        path_library_folder=home_dir,
        headless=True,
        use_case=use_case
    )

    queries = []
    for h in handles:
        if scrape_method == "timeline":
            query = int(h["twitter-id"])  # or whatever you'll add as the twitter ID
            queries.append(
                {"query": query,
                 "path": os.path.join(path_output_data,
                                      f"{h['ID']}_{h['SeedID']}_{h['Collection'].replace('_', '-')}_{h['Handle'].replace('_', '-')}_{start_date}_{end_date}.jsonl"),
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
                                          f"{h['ID']}_{h['SeedID']}_{h['Collection'].replace('_', '-')}_{h['Handle'].replace('_', '-')}_{start_date}_{end_date}.jsonl"),
                     "seed_info": h,
                     "start_date": start_date,
                     "end_date": end_date
                     }
                )
        else:
            raise ValueError(f"Scrape method must either be timeline or search !")

    # filter out the queries
    queries = [i for i in queries if not os.path.exists(i["path"])]

    # run it
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
    else:
        raise ValueError(f"Scrape method must either be timeline or search !")
    # show meta-data
    for k, v in scrape_meta_data["post_scraping_accounts"].items():
        logger.info(f"{k}: active {v}")
    """for key, value in scrape_meta_data.items():
        print(f"{key}")
        if isinstance(value, dict):
            for k, v in value.items():
                print(f"\t * {k}: {v}")
        else:
            print(f"\t{value}")"""

    return scrape_meta_data


def scrape_twitter():
    load_dotenv()

    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    os.environ["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY
    AWS_REGION = os.getenv("AWS_REGION")
    os.environ["AWS_REGION"] = AWS_REGION

    # time.sleep(int(np.random.randint(10, 30)*60))
    # input config
    config = configparser.ConfigParser()
    config_path = sys.argv[1]
    config.read(config_path)

    # make bucket environmental
    os.environ["S3BUCKET"] = config["s3paths"]["bucket"]
    os.environ["S3FOLDER"] = config["s3paths"]["folder"]
    os.environ["S3META_FOLDER"] = config["s3paths"]["meta-folder"]

    historical_twitter_scrape(config=config)


if __name__ == "__main__":
    scrape_twitter()
