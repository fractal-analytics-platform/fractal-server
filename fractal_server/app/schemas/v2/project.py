from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import field_validator
from pydantic.types import AwareDatetime

from .._validators import valstr


class ProjectCreateV2(BaseModel):

    model_config = ConfigDict(extra="forbid")

    name: str
    # Validators
    _name = field_validator("name")(classmethod(valstr("name")))


class ProjectReadV2(BaseModel):

    id: int
    name: str
    timestamp_created: AwareDatetime


class ProjectUpdateV2(BaseModel):

    model_config = ConfigDict(extra="forbid")

    name: Optional[str] = None
    # Validators
    _name = field_validator("name")(classmethod(valstr("name")))
