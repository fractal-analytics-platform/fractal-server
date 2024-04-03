from datetime import datetime
from typing import Optional

from pydantic import BaseModel
from pydantic import Extra
from pydantic import validator

from .._validators import valstr
from .._validators import valutc


class ProjectCreateV2(BaseModel, extra=Extra.forbid):

    name: str
    read_only: bool = False
    # Validators
    _name = validator("name", allow_reuse=True)(valstr("name"))


class ProjectReadV2(BaseModel):

    id: int
    name: str
    read_only: bool
    timestamp_created: datetime
    # Validators
    _timestamp_created = validator("timestamp_created", allow_reuse=True)(
        valutc("timestamp_created")
    )


class ProjectUpdateV2(BaseModel):

    name: Optional[str]
    read_only: Optional[bool]
    # Validators
    _name = validator("name", allow_reuse=True)(valstr("name"))
