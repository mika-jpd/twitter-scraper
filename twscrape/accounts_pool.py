import asyncio
import json
import random
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import TypedDict

from fake_useragent import UserAgent
from httpx import HTTPStatusError

from .account import Account
from .db import execute, fetchall, fetchone
from .logger import logger
from .login import LoginConfig, login
from .utils import get_env_bool, parse_cookies, utc

from hti import humanize, HTIOutput

# added by mika_jdp
import numpy as np


class NoAccountError(Exception):
    pass


class AccountInfo(TypedDict):
    username: str
    logged_in: bool
    active: bool
    last_used: datetime | None
    total_req: int
    error_msg: str | None


def guess_delim(line: str):
    lp, rp = tuple([x.strip() for x in line.split("username")])
    return rp[0] if not lp else lp[-1]


class AccountsPool:
    # _order_by: str = "RANDOM()"
    _order_by: str = "username"

    def __init__(
            self,
            db_file="accounts.db",
            login_config: LoginConfig | None = None,
            raise_when_no_account=False):
        self._db_file = db_file
        self._login_config = login_config or LoginConfig()
        self._raise_when_no_account = raise_when_no_account

        # added by mika_jpd
        self.endpoint_to_spread = {
            'SearchTimeline': 35
        }

    async def load_from_file(self, filepath: str, line_format: str):
        line_delim = guess_delim(line_format)
        tokens = line_format.split(line_delim)

        required = {"username", "password", "email", "email_password"}
        if not required.issubset(tokens):
            raise ValueError(f"Invalid line format: {line_format}")

        accounts = []
        with open(filepath, "r") as f:
            lines = f.read().split("\n")
            lines = [x.strip() for x in lines if x.strip()]

            for line in lines:
                data = [x.strip() for x in line.split(line_delim)]
                if len(data) < len(tokens):
                    raise ValueError(f"Invalid line: {line}")

                data = data[: len(tokens)]
                vals = {k: v for k, v in zip(tokens, data) if k != "_"}
                accounts.append(vals)

        for x in accounts:
            await self.add_account(**x)

    async def add_account(
            self,
            username: str,
            password: str,
            email: str,
            email_password: str,
            user_agent: str | None = None,
            proxy: str | None = None,
            cookies: str | None = None,
            mfa_code: str | None = None,

            # added by mika_jpd
            twofa_id: str | None = None,
            use_case: int | None = None,
            last_login: int | None = None,
            num_calls: int = 0
    ):
        qs = "SELECT * FROM accounts WHERE username = :username"
        rs = await fetchone(self._db_file, qs, {"username": username})
        if rs:
            logger.warning(f"Account {username} already exists")
            return

        account = Account(
            username=username,
            password=password,
            email=email,
            email_password=email_password,
            user_agent=user_agent or UserAgent().firefox,
            active=False,
            locks={},
            stats={},
            headers={},
            cookies=parse_cookies(cookies) if cookies else {},
            proxy=proxy,
            mfa_code=mfa_code,

            # added by mika_jpd
            num_calls=num_calls,
            in_use=False,
            twofa_id=twofa_id,
            use_case=use_case,
            last_login=last_login
        )

        if "ct0" in account.cookies:
            account.active = True

        await self.save(account)
        logger.info(f"Account {username} added successfully (active={account.active})")

    async def delete_accounts(self, usernames: str | list[str]):
        usernames = usernames if isinstance(usernames, list) else [usernames]
        usernames = list(set(usernames))
        if not usernames:
            logger.warning("No usernames provided")
            return

        qs = f"""DELETE FROM accounts WHERE username IN ({','.join([f'"{x}"' for x in usernames])})"""
        await execute(self._db_file, qs)

    async def delete_inactive(self):
        qs = "DELETE FROM accounts WHERE active = false"
        await execute(self._db_file, qs)

    async def get(self, username: str):
        qs = "SELECT * FROM accounts WHERE username = :username"
        rs = await fetchone(self._db_file, qs, {"username": username})
        if not rs:
            raise ValueError(f"Account {username} not found")
        return Account.from_rs(rs)

    async def get_all(self):
        qs = "SELECT * FROM accounts"
        rs = await fetchall(self._db_file, qs)
        return [Account.from_rs(x) for x in rs]

    async def get_account(self, username: str):
        qs = "SELECT * FROM accounts WHERE username = :username"
        rs = await fetchone(self._db_file, qs, {"username": username})
        if not rs:
            return None
        return Account.from_rs(rs)

    async def save(self, account: Account):
        data = account.to_rs()
        cols = list(data.keys())

        qs = f"""
        INSERT INTO accounts ({",".join(cols)}) VALUES ({",".join([f":{x}" for x in cols])})
        ON CONFLICT(username) DO UPDATE SET {",".join([f"{x}=excluded.{x}" for x in cols])}
        """
        await execute(self._db_file, qs, data)

    async def login(self, account: Account):
        try:
            await login(account, cfg=self._login_config)
            logger.info(f"Logged in to {account.username} successfully")
            return True
        except HTTPStatusError as e:
            rep = e.response
            logger.error(f"Failed to login '{account.username}': {rep.status_code} - {rep.text}")
            return False
        except Exception as e:
            logger.error(f"Failed to login '{account.username}': {e}")
            return False
        finally:
            await self.save(account)

    async def login_all(self, usernames: list[str] | None = None):
        if usernames is None:
            qs = "SELECT * FROM accounts WHERE active = false AND error_msg IS NULL"
        else:
            us = ",".join([f'"{x}"' for x in usernames])
            qs = f"SELECT * FROM accounts WHERE username IN ({us})"

        rs = await fetchall(self._db_file, qs)
        accounts = [Account.from_rs(rs) for rs in rs]
        # await asyncio.gather(*[login(x) for x in self.accounts])

        counter = {"total": len(accounts), "success": 0, "failed": 0}
        for i, x in enumerate(accounts, start=1):
            logger.info(f"[{i}/{len(accounts)}] Logging in {x.username} - {x.email}")
            status = await self.login(x)
            counter["success" if status else "failed"] += 1
        return counter

    async def relogin(self, usernames: str | list[str]):
        usernames = usernames if isinstance(usernames, list) else [usernames]
        usernames = list(set(usernames))
        if not usernames:
            logger.warning("No usernames provided")
            return

        qs = f"""
        UPDATE accounts SET
            active = false,
            locks = json_object(),
            last_used = NULL,
            error_msg = NULL,
            headers = json_object(),
            cookies = json_object(),
            user_agent = "{UserAgent().firefox}"
        WHERE username IN ({','.join([f'"{x}"' for x in usernames])})
        """

        await execute(self._db_file, qs)
        await self.login_all(usernames)

    async def relogin_failed(self):
        qs = "SELECT username FROM accounts WHERE active = false AND error_msg IS NOT NULL"
        rs = await fetchall(self._db_file, qs)
        await self.relogin([x["username"] for x in rs])

    async def reset_locks(self):
        qs = "UPDATE accounts SET locks = json_object()"
        await execute(self._db_file, qs)

    async def set_active(self, username: str, active: bool):
        qs = "UPDATE accounts SET active = :active WHERE username = :username"
        await execute(self._db_file, qs, {"username": username, "active": active})

    async def lock_until(self, username: str, queue: str, unlock_at: int, req_count=0):
        #  added by mika_jpd (added the )
        qs = f"""
        UPDATE accounts SET
            locks = json_set(locks, '$.{queue}', datetime({unlock_at}, 'unixepoch')),
            stats = json_set(stats, '$.{queue}', COALESCE(json_extract(stats, '$.{queue}'), 0) + {req_count}),
            last_used = datetime({utc.ts()}, 'unixepoch')        
        WHERE username = :username
        """
        await execute(self._db_file, qs, {"username": username})

    async def unlock(self, username: str, queue: str, req_count=0):
        qs = f"""
        UPDATE accounts SET
            locks = json_remove(locks, '$.{queue}'),
            stats = json_set(stats, '$.{queue}', COALESCE(json_extract(stats, '$.{queue}'), 0) + {req_count}),
            last_used = datetime({utc.ts()}, 'unixepoch')
        WHERE username = :username
        """
        await execute(self._db_file, qs, {"username": username})

    async def _get_and_lock(self, queue: str, condition: str):
        # if space in condition, it's a subquery, otherwise it's username
        condition = f"({condition})" if " " in condition else f"'{condition}'"

        # added by mika_jpd: modification of the locks so that it isn't +15 minutes but rather adds now + x seconds
        if queue in self.endpoint_to_spread:
            lock_until = np.random.normal(
                self.endpoint_to_spread[queue],
                np.absolute(self.endpoint_to_spread[queue] * 0.15)
            )
            lock_until = np.absolute(lock_until)  # time in seconds
        else:
            lock_until = 15 * 60  # 15 minutes in seconds

        # Todo update the number of calls
        if int(sqlite3.sqlite_version_info[1]) >= 35:
            qs = f"""
            UPDATE accounts SET
                locks = json_set(locks, '$.{queue}', datetime('now', '+{lock_until} seconds')),
                last_used = datetime({utc.ts()}, 'unixepoch'),
                in_use = true, 
                num_calls = num_calls + 1
            WHERE username = {condition}
            RETURNING *
            """
            rs = await fetchone(self._db_file, qs)
        else:
            tx = uuid.uuid4().hex
            qs = f"""
            UPDATE accounts SET
                locks = json_set(locks, '$.{queue}', datetime('now', '+{lock_until} seconds')),
                last_used = datetime({utc.ts()}, 'unixepoch'),
                in_use = true, 
                num_calls = num_calls + 1,
                _tx = '{tx}'
            WHERE username = {condition}
            """
            await execute(self._db_file, qs)

            qs = f"SELECT * FROM accounts WHERE _tx = '{tx}'"
            rs = await fetchone(self._db_file, qs)

        return Account.from_rs(rs) if rs else None

    async def get_for_queue(self, queue: str,
                            username: str = None,
                            use_case: int = None
                            ):
        """
        Fetches a username from accounts.db.
            -if username == None -> writes query to fetch the first free username,
            - else -> writes query to fetch the username with param username
        :param queue: the Twitter endpoint
        :type queue: str
        :param username: the username of the account to fetch. if None, fetches any account
        :type username: str
        :return: an account if the conditions are met else None
        :rtype: Account or None
        """

        # added by mika_jpd
        username_condition = "" if username is None else f"username = '{username}' AND"
        use_case_condition = "" if use_case is None else f"use_case = {use_case} AND"
        in_use_condition = f"in_use = false AND" if username is None else ""

        q = f"""
        SELECT username FROM accounts
        WHERE active = true AND {username_condition} {use_case_condition} {in_use_condition}(
            locks IS NULL
            OR json_extract(locks, '$.{queue}') IS NULL
            OR json_extract(locks, '$.{queue}') < datetime('now')
        )
        ORDER BY RANDOM()
        LIMIT 1
        """

        return await self._get_and_lock(queue, q)

    async def get_for_queue_or_wait(self,
                                    queue: str,
                                    username: str = None,
                                    use_case: int = None,
                                    _num_calls_before_humanization: tuple[int, int] = (15, 30)
                                    ) -> Account | None:
        msg_shown = False
        while True:
            account = await self.get_for_queue(queue=queue, username=username, use_case=use_case)
            if not account:
                if self._raise_when_no_account or get_env_bool("TWS_RAISE_WHEN_NO_ACCOUNT"):
                    raise NoAccountError(f"No account available for queue {queue}")

                if not msg_shown:
                    if username:  # if you're looking for a specific account
                        nat = await self.account_next_available_at(queue=queue, username=username)
                    else:  # if looking for a new account to attach to self.ctx
                        nat = await self.next_available_at(queue=queue)

                    if not nat:
                        logger.warning("No active accounts. Stopping...")
                        return None
                    if username:
                        msg = f'Account {username} not available for queue "{queue}", will be available at {nat}'
                    else:
                        msg = f'No account available for queue "{queue}". Next available at {nat}'
                    logger.info(msg)
                    msg_shown = True

                await asyncio.sleep(5)  # literally when the waiting happens
                continue
            else:
                # Todo check if accounts needs to be humanized
                if msg_shown:
                    logger.info(f"Continuing with account {account.username} on queue {queue}")
                if await self._account_needs_humanization(account=account,
                                                          _num_calls_before_humanization=_num_calls_before_humanization):
                    login_status: int = await self.humanize_account(account=account, queue=queue)
                    if login_status != 1:
                        return None

            account.last_used = datetime.utcnow()  # update its last use
            await self.save(account=account)
            return account

    async def next_available_at(self, queue: str):
        qs = f"""
        SELECT json_extract(locks, '$."{queue}"') as lock_until
        FROM accounts
        WHERE active = true AND json_extract(locks, '$."{queue}"') IS NOT NULL
        ORDER BY lock_until ASC
        LIMIT 1
        """
        rs = await fetchone(self._db_file, qs)
        if rs:
            now, trg = utc.now(), utc.from_iso(rs[0])
            if trg < now:
                return "now"

            at_local = datetime.now() + (trg - now)
            return at_local.strftime("%H:%M:%S")

        return None

    async def mark_inactive(self, username: str, error_msg: str | None):
        qs = """
        UPDATE accounts SET active = false, error_msg = :error_msg
        WHERE username = :username
        """
        await execute(self._db_file, qs, {"username": username, "error_msg": error_msg})

    async def stats(self):
        def locks_count(queue: str):
            return f"""
            SELECT COUNT(*) FROM accounts
            WHERE json_extract(locks, '$.{queue}') IS NOT NULL
                AND json_extract(locks, '$.{queue}') > datetime('now')
            """

        qs = "SELECT DISTINCT(f.key) as k from accounts, json_each(locks) f"
        rs = await fetchall(self._db_file, qs)
        gql_ops = [x["k"] for x in rs]

        config = [
            ("total", "SELECT COUNT(*) FROM accounts"),
            ("active", "SELECT COUNT(*) FROM accounts WHERE active = true"),
            ("inactive", "SELECT COUNT(*) FROM accounts WHERE active = false"),
            *[(f"locked_{x}", locks_count(x)) for x in gql_ops],
        ]

        qs = f"SELECT {','.join([f'({q}) as {k}' for k, q in config])}"
        rs = await fetchone(self._db_file, qs)
        return dict(rs) if rs else {}

    async def accounts_info(self):
        accounts = await self.get_all()

        items: list[AccountInfo] = []
        for x in accounts:
            item: AccountInfo = {
                "username": x.username,
                "logged_in": (x.headers or {}).get("authorization", "") != "",
                "active": x.active,
                "last_used": x.last_used,
                "total_req": sum(x.stats.values()),
                "error_msg": str(x.error_msg)[0:60],
            }
            items.append(item)

        old_time = datetime(1970, 1, 1).replace(tzinfo=timezone.utc)
        items = sorted(items, key=lambda x: x["username"].lower())
        items = sorted(
            items,
            key=lambda x: x["last_used"] or old_time if x["total_req"] > 0 else old_time,
            reverse=True,
        )
        items = sorted(items, key=lambda x: x["active"], reverse=True)
        # items = sorted(items, key=lambda x: x["total_req"], reverse=True)
        return items

    # added by mika_jpd
    async def account_next_available_at(self, queue: str, username: str):
        qs = f"""
                SELECT json_extract(locks, '$."{queue}"') as lock_until
                FROM accounts
                WHERE active = true AND json_extract(locks, '$."{queue}"') IS NOT NULL AND username = :username
                ORDER BY lock_until ASC
                LIMIT 1
                """
        rs = await fetchone(self._db_file, qs, {"username": username})
        if rs:
            now, trg = utc.now(), utc.from_iso(rs[0])
            if trg < now:
                return "now"

            at_local = datetime.now() + (trg - now)
            return at_local.strftime("%H:%M:%S")

        return None

    async def set_last_login(self, username: str, last_login: int) -> None:
        qs = f"""
                    UPDATE accounts SET last_login = :last_login WHERE username = :username
                    """
        await execute(self._db_file, qs, {'username': username, 'last_login': last_login})

    async def set_in_use(self, username: str, in_use: bool = False):
        qs = f"""
            UPDATE accounts SET in_use = :in_use
            WHERE username = :username
            """
        await execute(self._db_file, qs, {"username": username, 'in_use': in_use})

    async def get_active(self, use_case: int = None):
        if use_case is None:
            qs = "SELECT * FROM accounts WHERE active = true"
            rs = await fetchall(self._db_file, qs)
        else:
            qs = "SELECT * FROM accounts WHERE use_case = :use_case AND active = true"
            rs = await fetchall(self._db_file, qs, params={'use_case': use_case})
        return [Account.from_rs(x) for x in rs]

    async def set_use_case(self, username: str, use_case: int) -> None:
        qs = "UPDATE accounts SET use_case = :use_case WHERE username = :username"
        await execute(self._db_file, qs, {"username": username, "use_case": use_case})

    async def set_num_calls(self, username: str, num_calls: int = 0) -> None:
        qs = f"""UPDATE accounts SET num_calls = :num_calls WHERE username = :username"""
        await execute(self._db_file, qs, {'username': username, "num_calls": num_calls})

    async def _account_needs_humanization(self, account: Account,
                                          _num_calls_before_humanization: tuple[int, int]) -> bool:
        if account.num_calls > random.randint(*_num_calls_before_humanization):
            return True
        else:
            return False

    async def humanize_account(self, account: Account, queue: str) -> int:
        """
        Humanize the account and return its login status.
        :param account: the account to humanize
        :type account: Account
        :param queue: the endpoint e.g. SearchTimeline
        :type queue: str
        :return: The login status.
        :rtype: int
        """
        logger.info(f"Humanizing account {account.username} on queue {queue}.")
        result: HTIOutput = await humanize(acc=account)
        account.last_login = result.login_status  # update the login_status
        if result.login_status == 1:  # save cookies
            logger.info(
                f"Successfully humanized account {account.username} on queue {queue} with activities {result.activities}")
            account.cookies = result.cookies  # update the cookies
        else:
            logger.warning(f"Failed humanization {account.username} attempt with login status {result.login_status}")
        await self.save(account)
        return result.login_status
