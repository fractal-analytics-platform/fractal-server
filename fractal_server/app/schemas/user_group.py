from datetime import datetime

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_serializer
from pydantic.types import AwareDatetime

from fractal_server.types import ListUniqueAbsolutePathStr

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
    user_ids: list[int] | None = None
    viewer_paths: list[str]

    @field_serializer("timestamp_created")
    def serialize_datetime(v: datetime) -> str:
        return v.isoformat()


class UserGroupCreate(BaseModel):
    """
    Schema for `UserGroup` creation

    Attributes:
        name: Group name
    """

    model_config = ConfigDict(extra="forbid")

    name: str
    viewer_paths: ListUniqueAbsolutePathStr = Field(default_factory=list)


class UserGroupUpdate(BaseModel):
    """
    Schema for `UserGroup` update
    """

    model_config = ConfigDict(extra="forbid")

    viewer_paths: ListUniqueAbsolutePathStr = None
