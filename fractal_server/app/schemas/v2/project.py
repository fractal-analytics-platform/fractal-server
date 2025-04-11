from datetime import datetime
from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import field_serializer
from pydantic import field_validator
from pydantic.types import AwareDatetime

from .._validators import cant_set_none
from .._validators import NonEmptyString


class ProjectCreateV2(BaseModel):

    model_config = ConfigDict(extra="forbid")

    name: NonEmptyString


class ProjectReadV2(BaseModel):

    id: int
    name: str
    timestamp_created: AwareDatetime

    @field_serializer("timestamp_created")
    def serialize_datetime(v: datetime) -> str:
        return v.isoformat()


class ProjectUpdateV2(BaseModel):

    model_config = ConfigDict(extra="forbid")

    name: Optional[NonEmptyString] = None

    @field_validator("name")
    @classmethod
    def _cant_set_none(cls, v):
        return cant_set_none(v)
