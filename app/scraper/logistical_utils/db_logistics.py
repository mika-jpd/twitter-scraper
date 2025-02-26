import asyncio
import json
import sqlite3
from app.scraper.twscrape.utils import utc
from app.scraper.twscrape.accounts_pool import AccountsPool
from app.scraper.twscrape import API

def modify_column_old_to_new(path: str):
    with sqlite3.connect(path) as conn:
        c = conn.cursor()
        params = {"dict": json.dumps({})}
        q = """
        ALTER TABLE accounts
        RENAME COLUMN twofa_id TO twofa_id
        """
        c.execute(q, params)

        q = """
        ALTER TABLE accounts
        DROP COLUMN spread_locks
        """
        c.execute(q, params)

def modify_column_new_to_old(path: str):
    with sqlite3.connect(path) as conn:
        c = conn.cursor()
        params = {"dict": json.dumps({})}
        q = """
        ALTER TABLE accounts
        RENAME COLUMN twofa_id TO twofa_id;
        """
        c.execute(q, params)

        q = """
        ALTER TABLE accounts
        ADD spread_locks TEXT DEFAULT '{}' NOT NULL;
        """
        c.execute(q)

def queries_test(path: str, q:str, params:dict = None):
    with sqlite3.connect(path) as conn:
        c = conn.cursor()
        rs = c.execute(q, params).fetchall()
        pass

def create_query() -> tuple[str, dict]:
    condition = """
        SELECT username FROM accounts
        WHERE active = true AND username = :username AND  (
              locks IS NULL
              OR json_extract(locks, '$.UserByScreenName') IS NULL
              OR json_extract(locks, '$.UserByScreenName') < datetime('now')
            )
        ORDER BY RANDOM()
        LIMIT 1
        """
    condition = f"({condition})"
    lock_until = 2.3
    qs: str = f"""
        UPDATE accounts SET
            locks = json_set(locks, '$.UserByScreenName', datetime('now', '+10 seconds'))
        WHERE username = (
            SELECT username FROM accounts
            WHERE active = true AND username = :username AND  (
                json_extract(locks, '$.UserByScreenName') < datetime('now')
            )
            ORDER BY RANDOM()
            LIMIT 1
        )
        RETURNING *
    """
    params: dict = {"username": 'ShanaLui'}
    return qs, params

async def set_accounts_to_free(path: str):
    accounts_pool = AccountsPool(db_file=path)
    with sqlite3.connect(path) as conn:
        cur = conn.cursor()
        q = """SELECT username FROM accounts"""
        usernames = cur.execute(q).fetchall()
        usernames = [u[0] for u in usernames]
    for u in usernames:
        await accounts_pool.set_fingerprint(username=u)

async def delete_inactive_accounts(path: str):
    api = API(pool=path)
    await api.pool.delete_inactive()

if __name__ == "__main__":
    path = "/Users/mikad/MEOMcGill/twitter_scraper/database/accounts.db"
    modify_column_old_to_new(path)
    #modify_column_new_to_old(path=path)

    # query & test
    #qs, params = create_query()
    #test_queries(path=path, q=qs, params=params)
    # set_accounts free
    #asyncio.run(set_accounts_to_free(path))
    #asyncio.run(delete_inactive_accounts(path))