from TwitterScraper import TwitterScraper
from scraper.my_utils.meo_api import get_seeds
from scraper.my_utils.seed_manipulation.seeds import sort_seeds
import configparser
import sys
import asyncio
from dotenv import load_dotenv
from pathlib import Path
import datetime
import os


def daily_twitter_scrape(config: configparser.ConfigParser) -> dict:
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

    #path_setup = os.path.join(library_dir, "setup")
    path_logs = os.path.join(path_output, 'logs')

    # Todo: exclusively scrape once a day
    start_date = datetime.datetime.today() - datetime.timedelta(days=3)
    end_date = datetime.datetime.today() - datetime.timedelta(days=2)
    start_date = start_date.strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')

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

    if not (datetime.datetime.strptime(start_date, '%Y-%m-%d') < datetime.datetime.strptime(end_date, '%Y-%m-%d')):
        raise ValueError("Start date < end date.")

    try:
        query = config['query']['query']
    except KeyError:
        query = '(NOT Twitter.keyword:"")'

    print(os.path.dirname(os.path.abspath(__file__)))

    handles = get_seeds(
        username=os.getenv('MEO_USERNAME'),
        password=os.getenv('MEO_PASSWORD')
    )
    #handles = sort_seeds(handles)

    try:
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

    print(f"\t- home_dir: {home_dir}")
    print(f"\t- library_dir: {library_dir}")
    print(f"\t- path_output: {path_output}")
    print(f"\t- path_output_data: {path_output_data}")
    #print(f"\t- path_setup: {path_setup}")

    scraper = TwitterScraper(
        lim_acc=lim_acc,
        lim_browser=lim_browser,
        #setup_path=path_setup,
        path_library_folder=home_dir,
        headless=True,
        use_case=0
    )

    query = f'from:handle include:nativeretweets include:retweets until:{end_date} since:{start_date}'
    queries = [
        {"query": query.replace("handle", h["Handle"]),
         "path": os.path.join(path_output_data,
                              f"{h['ID']}_{h['SeedID']}_{h['Collection'].replace('_', '-')}_{h['Handle'].replace('_', '-')}_{start_date}_{end_date}.jsonl"),
         "seed_info": h,
         "start_date": start_date,
         "end_date": end_date
         }
        for h in handles
    ]

    # filter out the queries - need to do it with AWS next.
    queries = [i for i in queries if not os.path.exists(i["path"])]

    # run it
    scrape_meta_data: dict = asyncio.run(scraper.search_queries_scrape(queries=queries))

    # show meta-data
    for key, value in scrape_meta_data.items():
        print(f"{key}")
        if isinstance(value, dict):
            for k, v in value.items():
                print(f"\t * {k}: {v}")
        else:
            print(f"\t{value}")

    return scrape_meta_data


def scrape_twitter(config_path: str = "./configs/config_daily.ini"):
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
    config.read(config_path)

    # make bucket environmental
    os.environ["S3BUCKET"] = config["s3paths"]["bucket"]
    os.environ["S3FOLDER"] = config["s3paths"]["folder"]
    os.environ["S3META_FOLDER"] = config["s3paths"]["meta-folder"]

    daily_twitter_scrape(config=config)


if __name__ == "__main__":
    scrape_twitter()
