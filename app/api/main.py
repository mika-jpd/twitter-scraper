from fastapi import FastAPI
from rq.job import Job

from app.common.logger import setup_logging, get_logger
from app.common.utils import get_project_root
from app.api.router import router
import uvicorn
import os
import datetime

from app.common.queues import scheduler, scraper_queue

root_dir = get_project_root()
app_dir = os.path.join(root_dir, "app")
os.environ['PYTHONPATH'] = f"{app_dir}:{os.environ.get('PYTHONPATH', '')}"

# Setup logging for the API service
setup_logging()
logger = get_logger()

# Create FastAPI app
app = FastAPI()

# Import and include routers
logger.info("Loading router")
app.include_router(router, tags=["api"])


# Startup event
@app.on_event("startup")
async def lifespan():
    logger.info("Starting Scraper API service")
    # delete all scheduled jobs since we're re-running them
    try:
        logger.info("Attempting to get scheduled jobs")
        jobs: list[Job] = [i for i in scheduler.get_jobs()]
        logger.info(f"Found {len(jobs)} scheduled jobs")
        for job in jobs:
            logger.info(f"Deleting job {job.id} with func {job.func}, args {job.args} and kwargs {job.kwargs}")
            job.delete()
    except Exception as e:
        logger.error(f"Error during job cleanup: {str(e)}")

    now = datetime.datetime.utcnow()

    # daily
    scheduled_time_daily = datetime.datetime(year=now.year, month=now.month, day=now.day + 1, hour=11, minute=0,
                                             second=0, tzinfo=datetime.timezone.utc)
    interval_daily = 86400
    logger.info(
        f"Scheduling daily scrape job starting {scheduled_time_daily} (UTC) repeating every {interval_daily} seconds"
    )

    scheduler.schedule(
        scheduled_time=scheduled_time_daily,
        interval=interval_daily,  # 24 hours i.e. once a day
        func="app.worker.tasks.enqueue_front_with_unique_id",  # str path to the function
        args=["app.worker.tasks.run_scraper_daily", "scraper_queue"],
        kwargs={"query": "Platform:twitter AND NOT (MainType:news_outlet OR SubType:media)"},
        at_front=True,
        result_ttl=604800  # keep for 7 days
    )

    # tridaily
    scheduled_time_tridaily = datetime.datetime(year=now.year, month=now.month, day=now.day, hour=17, minute=40,
                                                second=0, tzinfo=datetime.timezone.utc)
    interval_tridaily = 3 * 86400  # 3*24 hours i.e. once every three days
    logger.info(
        f"Scheduling tridaily scrape job starting {scheduled_time_tridaily} (UTC) repeating every {interval_tridaily} seconds"
    )

    scheduler.schedule(
        scheduled_time=scheduled_time_tridaily,
        interval=interval_tridaily,
        func="app.worker.tasks.enqueue_front_with_unique_id",
        args=["app.worker.tasks.run_scraper_tridaily", "scraper_queue"],
        kwargs={"query": "Platform:twitter AND (MainType:news_outlet OR SubType:media)"},
        at_front=True,
        result_ttl=604800  # keep for 7 days
    )


# Health check endpoint
@app.get("/health")
async def health_check():
    return {"status": "healthy"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
