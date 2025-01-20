import os
import requests
import json
from my_utils.meo_api.get_token import get_token
import pandas as pd
from urllib.parse import urljoin
from datetime import datetime

# local
from my_utils.logger.logger import logger

def historical_seedlist(username: str = None,
                        password: str = None,
                        query: str = 'Platform:Twitter',
                        TOKEN: str = None,
                        verbose: bool = True,
                        only_actives: bool = True):
    if verbose:
        logger.info(f"Fetching historical seeds with query: {query}")
    base_url = "https://api.meoinsightshub.net"
    if TOKEN is None:
        TOKEN = get_token(base_url, username, password)
    headers = {
        "Authorization": f"Bearer {TOKEN}"  # Add Bearer token to the headers
    }
    params = {
        'query': query
    }

    response: requests.Response = requests.get(f"{base_url}/phh/historical_seedlist/", params=params, headers=headers)
    return response.json()
