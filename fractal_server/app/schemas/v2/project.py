from datetime import datetime

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import field_serializer
from pydantic.types import AwareDatetime

from fractal_server.types import NonEmptyStr
from fractal_server.types import SafeNonEmptyStr


class ProjectCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: SafeNonEmptyStr
    description: NonEmptyStr | None = None


class ProjectRead(BaseModel):
    id: int
    name: str
    description: str | None
    is_starred: bool
    timestamp_created: AwareDatetime

    @field_serializer("timestamp_created")
    def serialize_datetime(v: datetime) -> str:
        return v.isoformat()


class ProjectReadSuperuser(ProjectRead):
    user_email: str


class ProjectUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: SafeNonEmptyStr = None
    description: NonEmptyStr | None = None
