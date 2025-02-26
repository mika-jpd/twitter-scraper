import os
import requests

# local
from app.common.logger import get_logger

logger = get_logger()


def get_token(base_url=None, username=None, password=None):
    if base_url is None:
        base_url = "https://api.meoinsightshub.net"
    if username is None:
        username: str = os.getenv("MEO_USERNAME")
    if password is None:
        password: str = os.getenv("MEO_PASSWORD")
    params = {"username": username, "password": password}
    response = requests.post(f"{base_url}/meologin", params=params)
    if response.status_code == 200:
        return response.json()["access_token"]
    return None
