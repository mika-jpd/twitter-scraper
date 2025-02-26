import asyncio
import os
import aiohttp
import requests
from app.scraper.my_utils.meo_api.get_token import get_token

# local
from app.common.logger import get_logger

logger = get_logger()

def update_crawler_history(phh_id: str,
                           start_date: str,
                           end_date: str,
                           TOKEN=None,
                           verbose: bool = True,
                           username: str = None,
                           password: str = None) -> requests.Response:
    if verbose:
        logger.info(f"Updating crawler history for {phh_id}")
    base_url = "https://api.meoinsightshub.net"
    if TOKEN is None:
        TOKEN = get_token(base_url, username, password)
    headers = {
        "Authorization": f"Bearer {TOKEN}"  # Add Bearer token to the headers
    }

    params: dict = {
        "phh_id": phh_id,
        "start_date": start_date,
        "end_date": end_date
    }

    response: requests.Response = requests.get(f"{base_url}/phh/insert/crawler_history", params=params, headers=headers)
    if response.status_code != 200:
        logger.error(f"Failed {response.status_code} updating crawler history for {phh_id} from {start_date} to {end_date}")
    else:
        if verbose:
            logger.info(f"Updated {response.status_code} crawler history for {phh_id} from {start_date} to {end_date}")
    return response

async def update_crawler_history_async(session: aiohttp.ClientSession,
                                       sem: asyncio.Semaphore,
                                       TOKEN: str,
                                       phh_id: str,
                                       start_date: str,
                                       end_date: str) -> aiohttp.ClientResponse:
    base_url = "https://api.meoinsightshub.net"
    headers = {
        "Authorization": f"Bearer {TOKEN}"  # Add Bearer token to the headers
    }

    params: dict = {
        "phh_id": phh_id,
        "start_date": start_date,
        "end_date": end_date
    }
    async with sem and session.get(f"{base_url}/phh/insert/crawler_history", params=params,
                                   headers=headers) as response:
        if response.status != 200:
            logger.error(f"Failed updating crawler history for {phh_id} from {start_date} to {end_date}")
        else:
            logger.info(f"Updated crawler history for {phh_id} from {start_date} to {end_date}")
        return response