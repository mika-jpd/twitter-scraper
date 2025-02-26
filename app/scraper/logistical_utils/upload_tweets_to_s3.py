import datetime
import json

import tqdm

from app.scraper.my_utils.upload_to_s3.upload_to_s3 import upload_to_s3
import os
from dotenv import load_dotenv


def rename_tweet_paths(folder_path: str, old_name: str) -> str:
    old_full_path = os.path.join(folder_path, old_name)
    # open the dict
    tweet_dict = json.load(open(old_full_path, 'r'))
    # extract f"{h['ID']}_{h['SeedID']}_{h['Collection'].replace('_', '-')}_{h['Handle'].replace('_', '-')}_{start_date}_{end_date}.jsonl"),
    phh_id = tweet_dict["phh_id"]
    seed_id = tweet_dict["seed_id"]
    crawled_date = tweet_dict["crawled_date"]
    seed = tweet_dict["seed"]
    collection = tweet_dict["collection"].replace('_', '-')
    handle = seed["Handle"].replace('_', '-')

    # extract the tweet date
    tweet = tweet_dict["data"]
    start_date = datetime.datetime.strptime(tweet["date"], '%Y-%m-%d %H:%M:%S+00:00').date().strftime("%Y-%m-%d")
    end_date = (datetime.datetime.strptime(tweet["date"], '%Y-%m-%d %H:%M:%S+00:00').date() + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    tweet_id = tweet["id_str"]
    new_dict = {
        "phh_id": phh_id,
        "seed_id": seed_id,
        "collection": collection,
        "crawled_date": crawled_date,
        "data": tweet
    }
    json.dump(new_dict, open(old_full_path, 'w'))
    new_full_path = os.path.join(folder_path, f"{phh_id}_{seed_id}_{collection}_{handle}_{start_date}_{end_date}_{tweet_id}.json")
    return new_full_path


def get_tweets(tweets_folder_path: str) -> list[str]:
    # get & open the json files
    tweets_file_names = os.listdir(tweets_folder_path)
    old_tweet_full_paths = [os.path.join(tweets_folder_path, i) for i in tweets_file_names]

    # get new name
    new_tweet_full_paths = [rename_tweet_paths(tweets_folder_path, i) for i in tweets_file_names]

    # rename
    for new, old in zip(new_tweet_full_paths, old_tweet_full_paths):
        os.rename(old, new)

    return new_tweet_full_paths


# upload tweets to s3
def run():
    # AWS imperatives
    load_dotenv("/Users/mikad/MEOMcGill/twitter_scraper/scraper/.env")
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    os.environ["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY
    AWS_REGION = os.getenv("AWS_REGION")
    os.environ["AWS_REGION"] = AWS_REGION

    # path to scraped tweets
    path_scraped_tweets = "/Users/mikad/MEOMcGill/ecosystem-shift/data/temp/scraped_tweets"

    json_paths: list[str] = get_tweets(path_scraped_tweets)
    for p in tqdm.tqdm(json_paths):
        upload_to_s3(
            bucket_name="meo-raw-data",
            folder="twitter/tweets",
            filepath=p
        )

if __name__ == "__main__":
    run()