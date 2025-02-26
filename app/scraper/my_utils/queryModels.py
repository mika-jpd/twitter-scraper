from pydantic import BaseModel, model_validator
from typing import Optional, Callable


class TimelineQuery(BaseModel):
    query: int
    path: str
    seed_info: dict
    start_date: str
    end_date: str
    stopping_condition: Optional[Callable]
    update_phh_history: bool = True
    bool_upload_to_s3: bool = True
    bool_change_to_new_format: bool = True
    force_collection: bool = False

    @model_validator(mode="after")
    def validate_query(self) -> 'TimelineQuery':
        if not isinstance(self.query, int):
            raise ValueError(
                f"The query for a timeline query must be an int, currently self.query: {type(self.query)}={self.query}")
        return self

    def to_dict(self) -> dict:
        # Get all fields except stopping_condition using model_dump
        base_dict = self.model_dump(exclude={'stopping_condition'})
        if self.stopping_condition is not None:
            base_dict['stopping_condition'] = self.stopping_condition
        return base_dict


class SearchQuery(BaseModel):
    query: str
    path: str
    seed_info: dict
    start_date: str
    end_date: str
    update_phh_history: bool = True
    bool_upload_to_s3: bool = True
    bool_change_to_new_format: bool = True
    force_collection: bool = False

    def to_dict(self) -> dict:
        return self.model_dump()
