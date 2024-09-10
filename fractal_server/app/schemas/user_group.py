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
    Schema for `UserGroup` read

    NOTE: `user_ids` does not correspond to a column of the `UserGroup` table,
    but it is rather computed dynamically in relevant endpoints.

    Attributes:
        id: Group ID
        name: Group name
        timestamp_created: Creation timestamp
        user_ids: IDs of users of this group
    """

    id: int
    name: str
    timestamp_created: datetime
    user_ids: Optional[list[int]] = None


class UserGroupCreate(BaseModel, extra=Extra.forbid):
    """
    Schema for `UserGroup` creation

    Attributes:
        name: Group name
    """

    name: str


class UserGroupUpdate(BaseModel, extra=Extra.forbid):
    """
    Schema for `UserGroup` update

    NOTE: `new_user_ids` does not correspond to a column of the `UserGroup`
    table, but it is rather used to create new `LinkUserGroup` rows.

    Attributes:
        new_user_ids: IDs of groups to be associated to user.
    """

    new_user_ids: list[int] = Field(default_factory=list)
