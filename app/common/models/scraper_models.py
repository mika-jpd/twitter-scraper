from typing import Optional, Literal
import datetime
from pydantic import BaseModel, model_validator
from enum import Enum


class CustomQuery(BaseModel):
    query: str
    filename: str
    seed_info: dict = {}
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    update_phh_history: bool = False
    bool_upload_to_s3: bool = True
    bool_change_to_new_format: bool = False
    force_collection: bool = False


class Limits(BaseModel):
    accounts: int = 25
    browsers: int = 8
    queries: int = -1


class Paths(BaseModel):
    home: Optional[str] = None
    twitter_crawler: Optional[str] = None
    data: Optional[str] = None
    output: Optional[str] = None
    browsers: Optional[str] = None


class S3Paths(BaseModel):
    bucket: str = "meo-raw-data"
    folder: str = "twitter/tweets"
    meta_folder: str = "twitter/meta"


class Dates(BaseModel):
    start_date: str
    end_date: str


class ConfigModel(BaseModel):
    s3paths: Optional[S3Paths] = S3Paths()
    paths: Optional[Paths] = Paths()
    seed_query: Optional[str] = None
    custom_queries: Optional[list[CustomQuery]] = None
    custom_queries_dirname: Optional[str] = None
    use_case: Optional[int] = None
    limit: Optional[Limits] = Limits()
    dates: Optional[Dates] = None
    scrape_method: Literal["timeline", "search", "explore", "search_explore"] = "timeline"
    _log_level: str = "info"

    @model_validator(mode='after')
    def validate_queries(self) -> 'ConfigModel':
        if self.seed_query is None:  # sets seed_query default value
            self.seed_query = 'Platform:Twitter'
        if self.custom_queries:
            self.seed_query = None
        if self.dates is None and self.seed_query is None:
            raise ValueError('Error: seed_query and date are both required.')
        if self.custom_queries and self.custom_queries_dirname is None:
            raise ValueError('Error: Must provide a dirname for custom queries.')
        return self

    @model_validator(mode="after")
    def validate_dates(self) -> 'ConfigModel':
        if self.dates:
            if not (datetime.datetime.strptime(self.dates.start_date, '%Y-%m-%d') <= datetime.datetime.strptime(
                    self.dates.end_date, '%Y-%m-%d')):
                raise ValueError("Start date < end date.")
        return self


class JobStatus(str, Enum):
    QUEUED = "queued"
    STARTED = "started"
    FINISHED = "finished"
    FAILED = "failed"
    ALL = "all"
