import os
from dotenv import load_dotenv
import configparser
import sys
from pathlib import Path
from TwitterScraper import TwitterScraper
from logistical_utils.folder_manipulation import open_jsonl
import asyncio
from app.scraper.twscrape.logger import logger
from typing import Literal
import datetime

def scrape_queries():
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
    try:  # data
        data = config['paths']['data']
    except KeyError:
        data = "data"

    path_output = path_output
    path_setup = os.path.join(library_dir, "setup")
    path_data = os.path.join(path_output, data)
    path_logs = os.path.join(path_output, 'logs')

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

    print(os.path.dirname(os.path.abspath(__file__)))

    current_dt = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    path_log_for_this_run = os.path.join(path_logs, f'logs_scrape_queries_{current_dt}.log')

    # add local logging
    _LEVELS = Literal["TRACE", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    _LOG_LEVEL: _LEVELS = "INFO"
    logger.add(path_log_for_this_run, filter=lambda r: r["level"].no >= logger.level(_LOG_LEVEL).no)

    # create the output dir and the log files
    if not os.path.exists(path_output):
        os.mkdir(path_output)
    if not os.path.exists(path_data):
        os.mkdir(path_data)
    if not os.path.exists(path_logs):
        os.mkdir(path_logs)

    try:
        use_case = config['scrape_type']['use_case']
        if use_case == "None":
            use_case = None
        else:
            use_case = int(use_case)
    except KeyError:
        use_case = 2

    # get limit to the number of queries
    try:
        lim_queries: int = int(config["limit"]["queries"])
    except KeyError:
        lim_queries: None = None

    scraper = TwitterScraper(
        lim_acc=lim_acc,
        lim_browser=lim_browser,
        setup_path=path_setup,
        path_library_folder=home_dir,
        headless=True,
        use_case=use_case
    )

    # open the jsonl file with the queries
    path_queries = config["paths"]["queries"]  # fetch the paths to the queries in the config

    logger.info(f"\t- home_dir: {home_dir}")
    logger.info(f"\t- library_dir: {library_dir}")
    logger.info(f"\t- path_output: {path_output}")
    logger.info(f"\t- path_data: {path_data}")
    logger.info(f"\t- path_setup: {path_setup}")
    logger.info(f"\t- path_queries: {path_queries}")
    logger.info(f"\t- path_logs: {path_log_for_this_run}")

    queries = open_jsonl(path_queries)

    # modify the paths to add the current data path
    for i in queries:
        path_temp = i["path"]
        path_temp = os.path.join(path_data, path_temp)
        i["path"] = path_temp

    queries = [i for i in queries if not os.path.exists(i["path"])]
    total_num_queries = len(queries)
    if lim_queries is not None:
        queries = queries[:min(lim_queries, len(queries))]

    logger.info(f"\t- lim_queries: {lim_queries} / {total_num_queries}")

    # run it
    scrape_meta_data: dict = asyncio.run(scraper.search_scrape(queries=queries, use_case=use_case))
    for k, v in scrape_meta_data["post_scraping_accounts"].items():
        logger.info(f"{k}: active {v}")
"""    # show meta-data
    for key, value in scrape_meta_data.items():
        print(f"{key}")
        if isinstance(value, dict):
            for k, v in value.items():
                print(f"\t * {k}: {v}")
        else:
            print(f"\t{value}")"""


if __name__ == "__main__":
    scrape_queries()
