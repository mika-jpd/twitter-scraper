import asyncio
import json
from fastapi import FastAPI, HTTPException, APIRouter, status

import uuid
import datetime
from dateutil.tz import tz
import os
from typing import Optional

from app.common.logger import get_logger
from app.common.utils import get_project_root
from app.common.queues import scheduler
from rq.job import Job

# common to all endpoints
logger = get_logger()
home_dir = get_project_root()

# define the router
logger.info("Creating scheduler router")
router = APIRouter(prefix="/scheduler", tags=["scheduler"])


@router.post("/cancel/{job_id}")
async def quit_task_scheduler(job_id: str):
    try:
        # Fetch the job
        jobs: list[Job] = list(scheduler.get_jobs())  # returns None if there are no jobs

        if not jobs:
            raise HTTPException(
                status_code=404,
                detail=f"No scheduled jobs found"
            )

        # get the correct one
        job: Optional[Job] = next((j for j in jobs if j.id == job_id), None)

        if not job:
            raise HTTPException(
                status_code=404,
                detail=f"No scheduled jobs with id {job_id} found"
            )

        # Stop the job
        scheduler.cancel(job)

        job.meta['stopped'] = True
        job.save_meta()

        logger.info(f"Stopped scheduled job with ID: {job_id}")

        return {
            "status": "success",
            "message": f"Job {job_id} has been stopped"
        }
    except HTTPException:  # raises HTTP exceptions
        raise
    except Exception as e:  # raises all other exceptions
        logger.error(f"Failed to stop job {job_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to stop job: {str(e)}"
        )


@router.get("/jobs")
def scraper_scheduled_jobs(
        until: Optional[datetime.date] = None
):
    try:
        # until = None
        scheduled_jobs: list[tuple[Job, datetime.datetime]] = [i for i in scheduler.get_jobs(until=until, with_times=True)]
        logger.info(scheduled_jobs)
        # Validate that we got a response from the scheduler
        if scheduled_jobs is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Scheduler service is not responding"
            )
        # Convert jobs to serializable format
        serializable_jobs = [
            {
                "job_id": job[0].id,
                "func_name": job[0].func_name,
                "args": job[0].args,
                "scheduled_time (utc)": job[1].isoformat() if job[1] else None,
                "scheduled_time (US/Eastern)": job[1].replace(tzinfo=datetime.timezone.utc).astimezone(tz=tz.gettz('US/Eastern')).isoformat() if job[1] else None,
                "meta": job[0].meta
            }
            for job in scheduled_jobs
        ]

        return serializable_jobs
    except Exception as e:
        logger.error(f"Failed to get scheduled scraping jobs: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get scheduled scraping jobs: {str(e)}"
        )
