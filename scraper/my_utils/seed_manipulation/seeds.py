import os
import json


def sort_seeds(handles: list[dict[str, str]], path_setup: str = "./") -> list[dict[str, str]]:
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
