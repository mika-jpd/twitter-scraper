from app.common.logger import get_logger
from app.api.endpoints import scraper, accounts, scheduler
from fastapi import HTTPException, APIRouter, status
import time
from app.common.utils import get_project_root

logger = get_logger()

router = APIRouter()
logger.info("Loading scraper.router")
router.include_router(scraper.router)
logger.info("Loading accounts.router")
router.include_router(accounts.router)
logger.info("Loading scheduler.router")
router.include_router(scheduler.router)

home_dir = get_project_root()


def wait_for_job_completion(job, timeout=10):
    """Wait for job completion with timeout"""
    start_time = time.time()
    while time.time() - start_time < timeout:
        if job.is_finished:
            return job.result
        if job.is_failed:
            raise HTTPException(
                status_code=500,
                detail=f"Job failed: {job.exc_info}"
            )
        if job.is_stopped:
            raise Exception("Job was stopped")
        time.sleep(0.1)  # Short sleep to prevent CPU spinning

    raise TimeoutError(f"Job {job.id} timed out after {timeout} seconds")




