import asyncio
from twscrape import API, gather, set_log_level, logger
import sqlite3

async def main():
    api = API(use_case=0, pool="./accounts.db")
    logger.info("Starting !")
    tweets = await gather(
        api.search(q="(from:deputesPCF) until:2024-11-20 since:2024-11-15")
    )
    pass



if __name__ == "__main__":
    asyncio.run(main())