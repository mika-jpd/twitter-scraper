import os
import requests
from typing import Callable, Any
from app.scraper.my_utils.meo_api.get_token import get_token

# local
from app.common.logger import get_logger

logger = get_logger()

def search_scroll(query: str,
                  platform: str = "Twitter",
                  from_date: str = "",
                  to_date: str = "",
                  func_filter: Callable[[dict], Any] = lambda x: x,
                  verbose: bool = True,
                  username: str = None,
                  password: str = None) -> list[dict[str, str] | Any]:
    if verbose: logger.info(f"Running {query}")
    base_url = "https://api.meoinsightshub.net"
    TOKEN = get_token(base_url, username, password)
    headers = {
        "Authorization": f"Bearer {TOKEN}"  # Add Bearer token to the headers
    }

    all_data: list[dict] = []
    scroll_id = None
    params = {
        "platform": platform,
        "query": query,
        'from_date': from_date,
        'to_date': to_date,
        "size": 1000
    }
    while True:
        logger.info(f"Searching {query} for scroll {scroll_id}")
        response = requests.post(f'{base_url}/search_scroll',
                                 json=params,
                                 headers=headers,
                                 params={"scroll_id": scroll_id} if scroll_id else None)
        scroll_id = response.json()['scroll_id']  # get the scroll_id with the id returned by the endpoint call
        new_data = response.json()['data']
        for d in new_data:  # iterate through the new data and append to list
            all_data.append(func_filter(d))
        if response.json()["recordsFiltered"] == 0:
            logger.info(f"Searching {query} no more data recordsFiltered = 0")
            break

    return all_data