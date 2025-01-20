import os
import requests
from typing import Callable, Any
from my_utils.meo_api.get_token import get_token

# local
from my_utils.logger.logger import logger


def get_analysis_gap_detector(
        start_date: str,
        end_date: str,
        platforms: list[str] = None,
        collections: list[str] | None = None,
        username: str = None,
        password: str = None,
        TOKEN: str = None,
        verbose: bool = True, ) -> dict:
    if verbose: logger.info(f"/analysis_gap detector: collecting gaps for {collections}")
    base_url = "https://api.meoinsightshub.net"
    if TOKEN is None:
        TOKEN = get_token(base_url, username, password)
    headers = {
        "Authorization": f"Bearer {TOKEN}"  # Add Bearer token to the headers
    }

    if collections is None:
        collections = []
    if platforms is None:
        platforms = ["twitter"]
    json = {
        "analyze_start_date": start_date,
        "analyze_end_date": end_date,
        "analyze_collections": collections,
        "analyze_platforms": platforms,
    }

    response: requests.Response = requests.post(f"{base_url}/phh/analysis_gap_detector/",
                                                json=json,
                                                headers=headers)

    return response.json()
