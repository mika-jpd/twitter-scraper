import asyncio
import os
import requests
import json

# local
from my_utils.logger.logger import logger
from my_utils.meo_api.get_token import get_token

def get_seeds(username: str = None,
              password: str = None,
              query: str = 'Platform:Twitter',
              TOKEN: str = None,
              verbose: bool = True,
              only_actives: bool = True) -> list[dict[str, str]]:
    if verbose:
        logger.info(f"Fetching seeds with query: {query}")
    base_url = "https://api.meoinsightshub.net"
    if TOKEN is None:
        TOKEN = get_token(base_url, username, password)
    headers = {
        "Authorization": f"Bearer {TOKEN}"  # Add Bearer token to the headers
    }
    params = {
        'query': query,
        'only_actives': only_actives
    }

    response: requests.Response = requests.get(f"{base_url}/phh/seedlist/", params=params, headers=headers)
    response: list[dict[str, str]] = json.loads(response.text)
    return response

async def get_seeds_async(TOKEN: str,
                          sem: asyncio.Semaphore,
                          username: str = None,
                          password: str = None,
                          query: str = 'Platform:Twitter',
                          verbose: bool = True) -> list[dict[str, str]]:
    if verbose:
        logger.info(f"Fetching seeds with query: {query}")
    base_url = "https://api.meoinsightshub.net"
    if username is None:
        username: str = os.getenv("MEO_USERNAME")
    if password is None:
        password: str = os.getenv("MEO_PASSWORD")
    headers = {
        "Authorization": f"Bearer {TOKEN}"  # Add Bearer token to the headers
    }
    params = {
        'query': query
    }

    async with sem:
        response: requests.Response = requests.get(f"{base_url}/phh/seedlist/", params=params, headers=headers)
        response: list[dict[str, str]] = json.loads(response.text)
        return response