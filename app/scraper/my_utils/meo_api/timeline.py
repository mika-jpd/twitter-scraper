import os
import requests
from typing import Callable, Any
from app.scraper.my_utils.meo_api.get_token import get_token

# local
from app.common.logger import get_logger

logger = get_logger()

def get_timeline(
        query: str,
        platform: str = "Twitter",
        from_date: str = "",
        to_date: str = "",
        username: str = None,
        password: str = None,
        verbose: bool = True):
    if verbose: logger.info(f"/timeline: running {query}")
    base_url = "https://api.meoinsightshub.net"
    TOKEN = get_token(base_url, username, password)
    headers = {
        "Authorization": f"Bearer {TOKEN}"  # Add Bearer token to the headers
    }

    params = {
        "platform": platform,
        "query": query,
        "from_date": from_date,
        "to_date": to_date
    }
    response: requests.Response = requests.post(f"{base_url}/timeline", json=params, headers=headers)
    return response.json()["timeline"]

def get_timeline_advanced(
        query: str,
        agg_funct_field: str,
        agg_field: str,
        agg_funct: str,
        agg_time_interval: str="1d",
        platform: str = "Twitter",
        from_date: str = "",
        to_date: str = "",
        username: str = None,
        password: str = None,
        verbose: bool = True):
    if verbose: logger.info(f"/timeline: running {query}")
    base_url = "https://api.meoinsightshub.net"
    TOKEN = get_token(base_url, username, password)
    headers = {
        "Authorization": f"Bearer {TOKEN}"  # Add Bearer token to the headers
    }

    params = {
        "platform": platform,
        "query": query,
        "from_date": from_date,
        "to_date": to_date,
        "agg_funct_field": agg_funct_field,
        "agg_field": agg_field,
        "agg_funct": agg_funct,
        "agg_time_interval": agg_time_interval
    }
    response: requests.Response = requests.post(f"{base_url}/timeline_advanced", json=params, headers=headers)
    return response.json()["timeline"]

