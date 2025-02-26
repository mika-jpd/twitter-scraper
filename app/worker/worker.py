from rq import Worker
from app.common.logger import setup_logging, get_logger
from app.common.queues import scraper_queue, account_queue
from app.common.utils import get_project_root
from multiprocessing import Process
import os

# logging
setup_logging()
logger = get_logger()

root_dir = get_project_root()


def start_scraper_worker():
    try:
        logger.info("Starting scraper worker.")
        worker = Worker([scraper_queue], connection=scraper_queue.connection)
        logger.info("Scraper worker listening to scraper_queue")
        worker.work()
    except Exception as e:
        logger.error(f"Scraper worker failed to start: {str(e)}")
        raise


def start_account_worker():
    try:
        logger.info("Starting account worker.")
        worker = Worker([account_queue], connection=account_queue.connection)
        logger.info("Account worker listening to account_queue")
        worker.work()
    except Exception as e:
        logger.error(f"Account worker failed to start: {str(e)}")
        raise


if __name__ == '__main__':
    # Create processes for each worker
    scraper_process = Process(target=start_scraper_worker)
    account_process = Process(target=start_account_worker)

    # Start both processes
    scraper_process.start()
    account_process.start()

    # Wait for processes to complete
    scraper_process.join()
    account_process.join()
