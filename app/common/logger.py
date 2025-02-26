import sys
from typing import Literal
from loguru import logger
import contextvars
import os
from app.common.utils import get_project_root

_LEVELS = Literal["TRACE", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
_LOG_LEVEL: _LEVELS = "DEBUG"
_LEVEL_COLOR: dict[str, str] = {
    "INFO": "<green>",
    "WARNING": "<yellow>",
    "ERROR": "<red>",
    "DEBUG": "<blue>",
    "TRACE": "<white>"
}

for k, v in _LEVEL_COLOR.items():
    logger.level(k, color=v)

# Create a context variable to store job_id
current_job_id = contextvars.ContextVar('current_job_id', default=None)


def set_job_context(job_id: str):
    """Set the current job ID in context"""
    current_job_id.set(job_id)


def get_current_job_id():
    """Get the current job ID from context"""
    return current_job_id.get()


def setup_logging(job_id: str = None, _log_level: str = _LOG_LEVEL):
    """Initial logger setup"""
    logger.remove()  # Remove default handlers

    # Always add console logging
    logger.add(
        sys.stderr,
        level=_log_level,
        format="<level>"
               "{time:YYYY-MM-DD HH:mm:ss} | "
               "<b>{level}</b> | "
               "JOB:{extra[job_id]} | "
               "<cyan><i>{name}:{function}:{line}</i></cyan> - "
               "{message}"
               "</level>",
        enqueue=True,
        colorize=True
    )

    if job_id:
        set_job_context(job_id)
        home_dir = get_project_root()
        log_dir = os.path.join(home_dir, "output", "logs")
        log_file = os.path.join(log_dir, f"scraper_{job_id}.log")
        os.makedirs(log_dir, exist_ok=True)

        # Configure job-specific logger
        logger.add(
            log_file,
            rotation="100 MB",
            level=_LOG_LEVEL,
            format="<white>{time:YYYY-MM-DD HH:mm:ss}</white> | "
                   "<level>{level}</level> | "
                   "JOB:{extra[job_id]} | "
                   "{message}",
            enqueue=True,
            colorize=True
        )


# Create a logger that automatically includes job_id
def get_logger():
    """Get a logger instance that automatically includes job context"""
    job_id = get_current_job_id()
    if job_id:
        return logger.bind(job_id=job_id)
    return logger.bind(job_id="None")
