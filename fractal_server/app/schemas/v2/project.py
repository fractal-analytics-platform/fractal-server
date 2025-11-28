from datetime import datetime

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import field_serializer
from pydantic.types import AwareDatetime

from fractal_server.types import NonEmptyStr


class ProjectCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: NonEmptyStr


class ProjectRead(BaseModel):
    id: int
    name: str
    timestamp_created: AwareDatetime

    @field_serializer("timestamp_created")
    def serialize_datetime(v: datetime) -> str:
        return v.isoformat()


class ProjectUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: NonEmptyStr = None
