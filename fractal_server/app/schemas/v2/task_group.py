from datetime import datetime
from typing import Literal
from typing import Optional

from pydantic import BaseModel
from pydantic import validator

from .task import TaskReadV2


class TaskGroupCreateV2(BaseModel):
    active: bool = True
    origin: Literal["pypi", "wheel-file", "other"]
    pkg_name: str
    version: Optional[str] = None
    python_version: Optional[str] = None
    path: Optional[str] = None
    venv_path: Optional[str] = None
    pip_extras: Optional[str] = None


class TaskGroupReadV2(BaseModel):

    id: int
    task_list: list[TaskReadV2]

    user_id: int
    user_group_id: Optional[int] = None

    origin: Literal["pypi", "wheel-file", "other"]
    pkg_name: str
    version: Optional[str] = None
    python_version: Optional[str] = None
    path: Optional[str] = None
    venv_path: Optional[str] = None
    pip_extras: Optional[str] = None

    active: bool
    timestamp_created: datetime


class TaskGroupUpdateV2(BaseModel):
    user_group_id: Optional[int] = None
    active: Optional[bool] = None

    @validator("active")
    def active_cannot_be_None(cls, value):
        if value is None:
            raise ValueError("`active` cannot be set to None")
        return value
