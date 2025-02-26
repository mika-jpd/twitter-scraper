from fastapi import APIRouter, HTTPException
import uuid
import datetime
from rq import Retry
from rq.command import send_stop_job_command
from dateutil.tz import tz
import os
from typing import Optional

from app.common.logger import get_logger
from app.common.utils import get_project_root
from app.common.models.scraper_models import ConfigModel, JobStatus
from app.common.queues import scraper_queue, redis_client

# common to all endpoints
logger = get_logger()
home_dir = get_project_root()

# define the router
logger.info("Creating scraper router")
router = APIRouter(prefix="/scraper", tags=["scraper"])


@router.post("/start")
def scrape_twitter(config: ConfigModel):
    try:
        job_id = str(uuid.uuid4())
        job_metadata = {
            "timestamp (utc)": datetime.datetime.utcnow().isoformat(),
            "timestamp (US/Eastern)": datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).astimezone(
                tz=tz.gettz('US/Eastern')).isoformat(),
            "config": config.model_dump(),
            "job_id": job_id,
        }

        # create job
        job = scraper_queue.create_job(
            "app.worker.tasks.run_scraper",
            args=(job_metadata,),
            timeout="12h",  # it can be a string (e.g. '1h', '3m', '5s') - documentation
            result_ttl=604800,  # keep for 7 days
            failure_ttl=604800,  # keep for 7 days
            job_id=job_id,
            meta=job_metadata,
            retry=Retry(max=3, interval=60)
        )

        # enqueue job
        scraper_queue.enqueue_job(job)

        logger.info(f"Enqueued scraping job with ID: {job.id} and enforced job_id: {job_id}")
        return {
            "status": "success",
            "job_id": job.id,
            "message": "Scraping job successfully queued",
            "params": config.model_dump()
        }

    except Exception as e:
        logger.error(f"Failed to enqueue scraping job: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to start scraping job: {str(e)}"
        )


@router.get("/logs/{job_id}")
async def get_job_logs(job_id: str,
                       last_n: int = None,
                       level: str = None,
                       contains: str = None
                       ):
    try:
        # Construct the log file path
        log_file_path = os.path.join(home_dir, f"output/logs/scraper_{job_id}.log")

        # Check if log file exists
        if not os.path.exists(log_file_path):
            raise HTTPException(
                status_code=404,
                detail=f"No logs found for job {job_id}"
            )

        # Read the log file
        with open(log_file_path, 'r') as f:
            logs = f.readlines()

        # Clean the logs (remove empty lines and trailing whitespace)
        logs = [line.strip() for line in logs if line.strip()]

        # Clean the logs
        logs = [line.strip() for line in logs if line.strip()]

        # Apply filters if specified
        if level:
            logs = [log for log in logs if f"| {level.upper()} |" in log]

        if contains:
            logs = [log for log in logs if contains in log]

        # Get last N logs if specified
        if last_n:
            logs = logs[-last_n:]

        logger.info(f"Retrieved logs for job {job_id}")

        return {
            "logs": logs,
            "total_lines": len(logs)
        }

    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"Log file for job {job_id} not found"
        )
    except Exception as e:
        logger.error(f"Error retrieving logs for job {job_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve logs: {str(e)}"
        )


@router.post("/cancel/{job_id}")
async def quit_task(job_id: str):
    try:
        # Fetch the job
        job = scraper_queue.fetch_job(job_id)

        if not job:
            raise HTTPException(
                status_code=404,
                detail=f"Job with ID {job_id} not found"
            )

        # Check if job can be stopped
        if job.get_status() in ['finished', 'failed', 'stopped']:
            return {
                "status": "error",
                "message": f"Job already {job.get_status()}, cannot stop"
            }

        # Stop the job
        if job.get_status() == "queued":
            job.cancel()
        else:
            send_stop_job_command(redis_client, job_id=job_id)
        job.meta['stopped'] = True
        job.save_meta()

        logger.info(f"Stopped scraping job with ID: {job_id}")

        return {
            "status": "success",
            "message": f"Job {job_id} has been stopped"
        }

    except Exception as e:
        logger.error(f"Failed to stop job {job_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to stop job: {str(e)}"
        )


@router.get("/jobs")
async def get_jobs(status: Optional[JobStatus] = JobStatus.ALL, limit: Optional[int] = None):
    try:
        jobs_dict = {}

        # Get registries based on status
        if status == JobStatus.ALL or status == JobStatus.QUEUED:
            queued_ids = scraper_queue.get_job_ids()
            for job_id in queued_ids:
                job = scraper_queue.fetch_job(job_id)
                if job:
                    jobs_dict[job_id] = {
                        "status": "queued",
                        "enqueued_at": job.enqueued_at.isoformat() if job.enqueued_at else None,
                        "metadata": job.meta
                    }

        if status == JobStatus.ALL or status == JobStatus.STARTED:
            started_registry = scraper_queue.started_job_registry
            started_ids = started_registry.get_job_ids()
            for job_id in started_ids:
                job = scraper_queue.fetch_job(job_id)
                if job:
                    jobs_dict[job_id] = {
                        "status": "started",
                        "started_at": job.started_at.isoformat() if job.started_at else None,
                        "enqueued_at": job.enqueued_at.isoformat() if job.enqueued_at else None,
                        "metadata": job.meta
                    }

        if status == JobStatus.ALL or status == JobStatus.FINISHED:
            finished_registry = scraper_queue.finished_job_registry
            finished_ids = finished_registry.get_job_ids()
            for job_id in finished_ids:
                job = scraper_queue.fetch_job(job_id)
                if job:
                    jobs_dict[job_id] = {
                        "status": "finished",
                        "started_at": job.started_at.isoformat() if job.started_at else None,
                        "ended_at": job.ended_at.isoformat() if job.ended_at else None,
                        "result": job.result,
                        "metadata": job.meta
                    }

        if status == JobStatus.ALL or status == JobStatus.FAILED:
            failed_registry = scraper_queue.failed_job_registry
            failed_ids = failed_registry.get_job_ids()
            for job_id in failed_ids:
                job = scraper_queue.fetch_job(job_id)
                if job:
                    jobs_dict[job_id] = {
                        "status": "failed",
                        "started_at": job.started_at.isoformat() if job.started_at else None,
                        "ended_at": job.ended_at.isoformat() if job.ended_at else None,
                        "exc_info": job.exc_info,
                        "metadata": job.meta
                    }

        # Apply limit if specified
        if limit:
            jobs_dict = dict(list(jobs_dict.items())[:limit])

        logger.info(f"Found {len(jobs_dict)} jobs with status {status}")

        return jobs_dict

    except Exception as e:
        logger.error(f"Error fetching jobs: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch jobs: {str(e)}"
        )


@router.get("/logs")
async def list_logs():
    """Return list of all log files in the logs directory"""
    try:
        path_logs: str = os.path.join(get_project_root(), "output", "logs")
        log_files = sorted(os.listdir(path_logs),
                           key=lambda filename: os.stat(os.path.join(path_logs, filename)).st_mtime)
        log_files_and_modification_date = [
            {"file": f,
             "last_modificate_time (utc)": datetime.datetime.utcfromtimestamp(
                 os.stat(os.path.join(path_logs, f)).st_mtime).isoformat(),
             "last_modification_date (US/Eastern)": datetime.datetime.utcfromtimestamp(
                 os.stat(os.path.join(path_logs, f)).st_mtime).replace(
                 tzinfo=datetime.timezone.utc).astimezone(
                 tz=tz.gettz('US/Eastern')
             )
             } for f in log_files]
        return log_files_and_modification_date
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error accessing logs directory: {str(e)}")


# Todo: add the number of files in each folder
@router.get("/data")
async def list_logs():
    """Return list of all log files in the logs directory"""
    try:
        path_data: str = os.path.join(get_project_root(), "output", "data")
        log_files = sorted(os.listdir(path_data),
                           key=lambda filename: os.stat(os.path.join(path_data, filename)).st_mtime)
        data_files_and_modification_date = [
            {"file": f,
             "last_modificate_time (utc)": datetime.datetime.utcfromtimestamp(
                 os.stat(os.path.join(path_data, f)).st_mtime).isoformat(),
             "last_modification_date (US/Eastern)": datetime.datetime.utcfromtimestamp(
                 os.stat(os.path.join(path_data, f)).st_mtime).replace(
                 tzinfo=datetime.timezone.utc).astimezone(
                 tz=tz.gettz('US/Eastern')
             )
             } for f in log_files]
        return data_files_and_modification_date
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error accessing logs directory: {str(e)}")
