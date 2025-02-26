from datetime import datetime
from pydantic import BaseModel, create_model
from app.scraper.twscrape import Account
from typing import get_type_hints, Optional


class CookieModel(BaseModel):
    ct0: str
    auth_token: str

    class Config:
        extra = "allow"


class NewTwitterAccountModel(BaseModel):
    username: str
    password: str
    email: str
    email_password: str
    use_case: int
    cookies: CookieModel
    automated: bool


class TwscrapeAccountModel(BaseModel):
    username: str
    password: str
    email: str
    email_password: str
    user_agent: str
    active: bool
    locks: dict[str, datetime] = {}
    stats: dict[str, int] = {}
    headers: dict[str, str] = {}
    cookies: dict[str, str] = {}
    twofa_id: Optional[str] = None
    proxy: Optional[str] = None
    error_msg: Optional[str] = None
    last_used: Optional[datetime] = None
    in_use: bool = False
    use_case: Optional[int] = None
    last_login: Optional[int] = None
    num_calls: Optional[int] = None
    automated: bool = False
    _tx: Optional[str] = None

    class Config:
        from_attributes = True  # allows conversion from ORM objects
