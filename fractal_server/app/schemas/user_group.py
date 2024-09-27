from datetime import datetime
from typing import Optional

from pydantic import BaseModel
from pydantic import Extra
from pydantic import Field
from pydantic import validator

from ._validators import val_absolute_path
from ._validators import val_unique_list

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
    viewer_paths: list[str]


class UserGroupCreate(BaseModel, extra=Extra.forbid):
    """
    Schema for `UserGroup` creation

    Attributes:
        name: Group name
    """

    name: str
    viewer_paths: list[str] = Field(default_factory=list)

    @validator("viewer_paths")
    def viewer_paths_validator(cls, value):
        for i, path in enumerate(value):
            value[i] = val_absolute_path(f"viewer_paths[{i}]")(path)
        value = val_unique_list("viewer_paths")(value)
        return value


class UserGroupUpdate(BaseModel, extra=Extra.forbid):
    """
    Schema for `UserGroup` update

    NOTE: `new_user_ids` does not correspond to a column of the `UserGroup`
    table, but it is rather used to create new `LinkUserGroup` rows.

    Attributes:
        new_user_ids: IDs of groups to be associated to user.
    """

    new_user_ids: list[int] = Field(default_factory=list)
    viewer_paths: Optional[list[str]] = None

    _val_unique = validator("new_user_ids", allow_reuse=True)(
        val_unique_list("new_user_ids")
    )

    @validator("viewer_paths")
    def viewer_paths_validator(cls, value):
        for i, path in enumerate(value):
            value[i] = val_absolute_path(f"viewer_paths[{i}]")(path)
        value = val_unique_list("viewer_paths")(value)
        return value
