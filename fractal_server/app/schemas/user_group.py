from datetime import datetime
from typing import Optional

from pydantic import BaseModel
from pydantic import Extra
from pydantic import Field

__all__ = (
    "UserGroupRead",
    "UserGroupUpdate",
    "UserGroupCreate",
)


class UserGroupRead(BaseModel):
    """
    FIXME GROUPS: docstring
    """

    id: int
    name: str
    timestamp_created: datetime
    user_ids: Optional[list[int]] = None


class UserGroupCreate(BaseModel, extra=Extra.forbid):
    """
    FIXME GROUPS: docstring
    FIXME GROUPS: add validator for `name`
    """

    name: str


class UserGroupUpdate(BaseModel, extra=Extra.forbid):
    """
    FIXME GROUPS: docstring
    """

    new_user_ids: list[int] = Field(default_factory=list)
