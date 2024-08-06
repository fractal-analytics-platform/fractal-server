from datetime import datetime
from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import field_validator

from .._validators import valstr
from .._validators import valutc


class ProjectCreateV2(BaseModel):

    model_config = ConfigDict(extra="forbid")

    name: str
    # Validators
    _name = field_validator("name")(valstr("name"))


class ProjectReadV2(BaseModel):

    id: int
    name: str
    timestamp_created: datetime
    # Validators
    _timestamp_created = field_validator("timestamp_created")(
        valutc("timestamp_created")
    )


class ProjectUpdateV2(BaseModel):

    name: Optional[str] = None
    # Validators
    _name = field_validator("name")(valstr("name"))
