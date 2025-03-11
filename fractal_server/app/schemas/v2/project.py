from datetime import datetime
from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import field_serializer
from pydantic.types import AwareDatetime

from .._validators import String


class ProjectCreateV2(BaseModel):

    model_config = ConfigDict(extra="forbid")

    name: String


class ProjectReadV2(BaseModel):

    id: int
    name: str
    timestamp_created: AwareDatetime

    @field_serializer("timestamp_created")
    def serialize_datetime(v: datetime) -> str:
        return v.isoformat()


class ProjectUpdateV2(BaseModel):

    model_config = ConfigDict(extra="forbid")

    name: Optional[String] = None
