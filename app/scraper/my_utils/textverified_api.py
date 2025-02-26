import asyncio
import datetime
import os
from typing import Optional

import httpx
import requests
from dotenv import load_dotenv
from httpx import AsyncClient

load_dotenv("../.env")

BASE_URL = "https://www.textverified.com"
API_KEY = os.getenv("TEXT_VERIFIED_API_KEY")
EMAIL = os.getenv("TEXT_VERIFIED_EMAIL")


async def get_bearer_token() -> str:
    headers = {
        "X-API-KEY": API_KEY,
        "X-API-USERNAME": EMAIL
    }
    async with httpx.AsyncClient() as client:
        response = await client.post(
            url=f"{BASE_URL}/api/pub/v2/auth",
            headers=headers
        )
        if response.status_code == 200:
            return response.json().get("token")


async def get_account_details(bearer_token) -> dict:
    headers = {"Authorization": f"Bearer {bearer_token}"}
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{BASE_URL}/api/pub/v2/account/me", headers=headers)
        response.raise_for_status()
        data = response.json()
        return data


async def create_verification(bearer_token, service_name: str = "twitter") -> str:
    """Creates a verification to use
    Params:
    - bearer_token: return value from generate_bearer_token
    """
    headers = {"Authorization": f"Bearer {bearer_token}"}

    service_name = service_name # get the name from get_service_list
    json_data = {
        "serviceName": service_name,
        "capability": "sms", # sms, voice, smsAndVoiceCombo
    }
    async with httpx.AsyncClient() as client:
        r = await client.post(f"{BASE_URL}/api/pub/v2/verifications", headers=headers, json=json_data)
        #r.raise_for_status()
        data = r.json()
        return data.get("href")

async def check_verification_is_completed(data: dict):
    """Params:
    - data: the data from get_verification_details
    Return:
    - bool: True if verification completed, False if not
    """
    verificationState = data.get("state")

    if verificationState == "verificationPending":
        return False
    elif verificationState == "verificationCompleted":
        return True


async def get_verification_details(bearer_token, href):
    """Returns either a pending status verification, or a completed status verification
    Params:
    - bearer_token: return value from generate_bearer_token
    - href: verification href value from create_verification, if not given,
    then will generate a verification
    Return:
    - bool: True if verification is completed, False if not
    """
    headers = {"Authorization": f"Bearer {bearer_token}"}

    async with AsyncClient() as client:
        r = await client.get(href, headers=headers)
        #r.raise_for_status()

    data = r.json()
    return data

async def get_correct_recent_sms_verification_code(verif_dict: dict, seconds: int = 180) -> Optional[str]:
    verif_dict = verif_dict.get("data")
    if verif_dict is None:
        return None

    # get the most recent
    verif_dict = sorted(
        verif_dict,
        key=lambda x: datetime.datetime.strptime(x["createdAt"], "%Y-%m-%dT%H:%M:%S.%f+00:00"),
        reverse=True
    )
    latest_verification = verif_dict[0]
    created_at = datetime.datetime.strptime(latest_verification.get("createdAt"), "%Y-%m-%dT%H:%M:%S.%f+00:00")

    # check if it's in the last 3 minutes
    if created_at > (datetime.datetime.now() - datetime.timedelta(seconds=seconds)):
        return latest_verification.get("parsedCode")
    else:
        return None

async def run():
    bearer_token = await get_bearer_token()
    #href = await create_verification(bearer_token)
    #print(href)
    creds = await get_verification_details(bearer_token, href="https://www.textverified.com/api/pub/v2/sms?Reservation|d=Ir_01JJZ7HJYVB3YZOPS3GG5XX2XG")
    x = await get_correct_recent_sms_verification_code(creds, seconds=345600)
    print(creds)
    pass
if __name__ == "__main__":
    asyncio.run(run())