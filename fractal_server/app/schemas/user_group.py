from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator
from pydantic.types import AwareDatetime

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
    timestamp_created: AwareDatetime
    user_ids: Optional[list[int]] = None
    viewer_paths: list[str]


class UserGroupCreate(BaseModel):
    """
    Schema for `UserGroup` creation

    Attributes:
        name: Group name
    """

    model_config = ConfigDict(extra="forbid")

    name: str
    viewer_paths: list[str] = Field(default_factory=list)

    @field_validator("viewer_paths")
    @classmethod
    def viewer_paths_validator(cls, value):
        for i, path in enumerate(value):
            value[i] = val_absolute_path(f"viewer_paths[{i}]")(path)
        value = val_unique_list("viewer_paths")(value)
        return value


class UserGroupUpdate(BaseModel):
    """
    Schema for `UserGroup` update
    """

    model_config = ConfigDict(extra="forbid")

    viewer_paths: Optional[list[str]] = None

    @field_validator("viewer_paths")
    @classmethod
    def viewer_paths_validator(cls, value):
        for i, path in enumerate(value):
            value[i] = val_absolute_path(f"viewer_paths[{i}]")(path)
        value = val_unique_list("viewer_paths")(value)
        return value
