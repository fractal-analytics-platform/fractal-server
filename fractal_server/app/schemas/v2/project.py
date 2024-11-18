from datetime import datetime
from typing import Optional

from pydantic import BaseModel
from pydantic import Extra
from pydantic import validator

from .._validators import valstr


class ProjectCreateV2(BaseModel, extra=Extra.forbid):

    name: str
    # Validators
    _name = validator("name", allow_reuse=True)(valstr("name"))


class ProjectReadV2(BaseModel):

    id: int
    name: str
    timestamp_created: datetime


class ProjectUpdateV2(BaseModel, extra=Extra.forbid):

    name: Optional[str]
    # Validators
    _name = validator("name", allow_reuse=True)(valstr("name"))
