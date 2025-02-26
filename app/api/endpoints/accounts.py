import datetime
import json
from fastapi import APIRouter, HTTPException
import os
from typing import Optional

from app.common.logger import get_logger
from app.common.utils import get_project_root
from app.common.models.accounts_models import NewTwitterAccountModel, CookieModel, TwscrapeAccountModel
from app.scraper.twscrape.account import Account
from app.scraper.twscrape.api import API

# common to all endpoints
logger = get_logger()
home_dir = get_project_root()

# define the router
logger.info("Creating accounts router")
router = APIRouter(prefix="/accounts", tags=["accounts"])


def get_twscrape_api(use_case: Optional[int] = None, db_file: str = "accounts.db"):
    path = os.path.join(get_project_root(), "db", db_file)
    return API(
        use_case=use_case,
        pool=path
    )


@router.post("/add")
async def add_account(account: NewTwitterAccountModel, replace: bool = False):
    try:
        api: API = get_twscrape_api()

        # process cookies
        cookies = account.cookies.model_dump()  # CookieModel ensures it has ct0 and auth_token

        # check if account already exists
        account_from_db: Optional[Account] = await api.pool.get_account(account.username)

        if not replace and account_from_db is not None:
            raise HTTPException(status_code=404,
                                detail=f"Account {account.username} already exists and replace is set to {replace}."
                                       f"If you want to add this account please set replace to {True}")

        await api.pool.add_account(
            username=account.username,
            password=account.password,
            email=account.email,
            email_password=account.email_password,
            cookies=json.dumps(cookies),
            use_case=account.use_case,
            automated=account.automated
        )

        # check if it was successfully added
        account_from_db: Optional[Account] = await api.pool.get_account(account.username)
        if account_from_db is None:
            raise HTTPException(status_code=404,
                                detail=f"Failed to add account {account.username} to the database; "
                                       f"searching for it yields {account_from_db}.")

        # Return the job result
        logger.info(f"Account {account.username} added successfully.")
        return {
            "success": True,
            "message": f"Account {account.username} added successfully."
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to add account: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to add account: {str(e)}"
        )


@router.post("/save")
async def save_account(account: TwscrapeAccountModel):
    try:
        api: API = get_twscrape_api()

        # check if account exists
        account_from_db: Optional[Account] = await api.pool.get_account(account.username)

        if account_from_db is None:
            raise HTTPException(status_code=404,
                                detail=f"Could not find account {account.username}. "
                                       f"Please use the /add endpoint if you wish to create it")

        # now save the modified account
        acc_twscrape: Account = Account(**account.model_dump())
        if isinstance(account.last_used, str) and not isinstance(acc_twscrape.last_used, datetime.datetime):
            acc_twscrape.last_used = datetime.datetime.strptime(account.last_used, "%Y-%m-%dT%H:%M:%S.%f+00:00")
        await api.pool.save(acc_twscrape)
        logger.info(f"Account {account.username} was successfully saved")
        return {
            "success": True,
            "message": f"Account {account.username} was successfully saved.",
            "account_model": account.model_dump(),
            "account_twscrape": acc_twscrape.dict()
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to save account {account}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to save account {account}: {str(e)}"
        )


# set an account to active
@router.post("/set_active/{username}")
async def set_active(username: str, active: bool, error_msg: str = None, log_level: str = 'INFO'):
    try:
        api: API = get_twscrape_api()

        # check if account exists
        account_from_db: Optional[Account] = await api.pool.get_account(username)

        if account_from_db is None:
            raise HTTPException(status_code=404,
                                detail=f"Could not find account {username}")

        await api.pool.set_active(
            username=username,
            active=active,
            error_msg=error_msg
        )

        # check if it was successfully added
        account_from_db: Optional[Account] = await api.pool.get_account(username)
        if not (bool(account_from_db.active) == bool(active)):
            raise HTTPException(status_code=404,
                                detail=f"Failed to set account's active status to {active};"
                                       f"db status {bool(account_from_db.active)} is not equal to {bool(active)}")

        # Return the job result
        logger.info(f"Active status of account {username} successfully set to {bool(active)}.")
        return {
            "success": True,
            "message": f"Active status of account {username} successfully set to {bool(active)}."
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to set active status of account {username} to {active}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to set active status of account {username} to {active}: {str(e)}"
        )


# update cookies
@router.post("/update_cookies/{username}")
async def update_cookies(username: str, cookies: CookieModel, log_level: str = 'INFO'):
    try:
        api: API = get_twscrape_api()

        # check if account exists
        account_from_db: Optional[Account] = await api.pool.get_account(username)

        if account_from_db is None:
            raise HTTPException(status_code=404,
                                detail=f"Could not find account {username}")

        await api.pool.set_cookies(
            username=username,
            cookies=cookies.model_dump()
        )

        # Return the job result
        logger.info(f"Account {username} cookies successfully set to {cookies.model_dump()}")
        return {
            "success": True,
            "message": f"Account {username} cookies successfully set to {cookies.model_dump()}"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to set cookies of account {username} to {cookies.model_dump()}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to set cookies of account {username} to {cookies.model_dump()}: {str(e)}"
        )


# set in use
@router.post("/set_in_use/{username}")
async def set_in_use(username: str, in_use: bool, log_level: str = 'INFO'):
    try:
        api: API = get_twscrape_api()

        # check if account exists
        account_from_db: Optional[Account] = await api.pool.get_account(username)

        if account_from_db is None:
            raise HTTPException(status_code=404,
                                detail=f"Could not find account {username}")

        await api.pool.set_in_use(
            username=username,
            in_use=in_use
        )

        # check if it was successfully added
        account_from_db: Optional[Account] = await api.pool.get_account(username)
        if not (bool(account_from_db.in_use) == bool(in_use)):
            raise HTTPException(status_code=404,
                                detail=f"Failed to set account's in_use status to {in_use};"
                                       f"db status {bool(account_from_db.in_use)} is not equal to {bool(in_use)}")

        # Return the job result
        logger.info(f"Account {username} in_use status successfully set to {in_use}.")
        return {
            "success": True,
            "message": f"Account {username} in_use status successfully set to {in_use}."
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to set in_use status of account {username} to {in_use}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to set in_use status of account {username} to {in_use}: {str(e)}"
        )


@router.get("/")
async def get_accounts(active: Optional[bool] = None, use_case: Optional[int] = None, log_level: str = 'INFO'):
    try:
        api: API = get_twscrape_api()

        # check if account exists
        accounts: list[Account] = await api.pool.get_all()
        if active is not None:
            accounts: list[Account] = [a for a in accounts if bool(a.active) == active]
        if use_case is not None:
            accounts: list[Account] = [a for a in accounts if int(a.use_case) == use_case]

        # Return the job result
        logger.info(f"Fetched {len(accounts)} from db")
        return {
            "success": True,
            "message": f"Fetched {len(accounts)} from db",
            "accounts": [a.dict() for a in accounts]
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch accounts from db: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch accounts from db: {str(e)}"
        )
