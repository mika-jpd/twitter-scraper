import asyncio
import json
import os.path
from typing import Optional

from app.scraper.twscrape import Tweet
from app.scraper.twscrape.api import API
from app.scraper.hti.humanTwitterInteraction import humanize, HTIOutput


def fetch_tweets_to_scrape() -> dict:
    path_tweets = "/Users/mikad/MEOMcGill/ecosystem-shift/data/temp"
    scraped_tweets: list[str] = []
    path_tweets_to_scrape = os.path.join(path_tweets, "tweets_to_scrape")
    for i in os.listdir(os.path.join(path_tweets, "scraped_tweets")):
        with open(os.path.join(path_tweets, "scraped_tweets", i), "r") as f:
            j = json.load(f)
            try:
                id_ = j["data"]["id_str"]
                scraped_tweets.append(id_)
                if os.path.exists(os.path.join(path_tweets_to_scrape, f"{id_}.json")):
                    os.remove(os.path.join(path_tweets_to_scrape, f"{id_}.json"))
                    pass
            except KeyError:
                pass

    tweets_to_scrape = {
        i.replace(".json", ""): json.load(open(os.path.join(path_tweets_to_scrape, i)))
        for i in os.listdir(path_tweets_to_scrape)
    }
    tweets_to_scrape = {
        k: v for k, v in tweets_to_scrape.items()
        if k not in scraped_tweets
    }
    return tweets_to_scrape

async def save_tweet(path: str, tweet: dict) -> None:
    with open(path, "w+") as f:
        json.dump(tweet, f)

async def get_tweet(t_dict: dict, api: API, sem: asyncio.Semaphore) -> None:
    async with sem:
        path = "/Users/mikad/MEOMcGill/ecosystem-shift/data/temp/scraped_tweets"
        id_ = int(t_dict["tweet_id"])
        tweet: Optional[Tweet] = await api.tweet_details(id_)
        if tweet:
            t_dict = t_dict["format"]
            t_dict["data"] = tweet.dict()
            await save_tweet(path=os.path.join(path, f"{id_}.json"), tweet=t_dict)

            # delete the tweet in tweets_to_scrape
            full_path = os.path.join("/Users/mikad/MEOMcGill/ecosystem-shift/data/temp", "tweets_to_scrape", f"{str(id_)}.json")
            if os.path.exists(full_path):
                os.remove(full_path)
    pass

async def run():
    tweets = fetch_tweets_to_scrape()
    num_calls_sem = asyncio.Semaphore(15)
    num_browsers_sem = asyncio.Semaphore(7)
    api = API(
        pool="/Users/mikad/MEOMcGill/twitter_scraper/database/accounts_db_1_acc.db",
        _num_calls_before_humanization=(10000, 12000),
        sem=num_calls_sem
    )
    # login to all of them
    accs = await api.pool.get_active()
    tasks = []
    for a in accs:
        tasks.append(humanize(acc=a, size=1, sem=num_browsers_sem, headless=False))
    results: tuple[HTIOutput] = await asyncio.gather(*tasks)
    for r in results:
        username: str = r.username
        if r.cookies:
            await api.pool.set_cookies(username, r.cookies)
            await api.pool.set_in_use(username=username, in_use=False)
            await api.pool.set_active(username=username, active=True)

    tasks = []
    for k, v in tweets.items():
        tasks.append(get_tweet(v, api, sem=num_calls_sem))
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(run())
