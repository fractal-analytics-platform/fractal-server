from datetime import datetime
from typing import Optional

from pydantic import BaseModel
from pydantic import validator

from ._validators import valstr
from ._validators import valutc


__all__ = (
    "ProjectCreate",
    "ProjectRead",
    "ProjectUpdate",
)


class _ProjectBase(BaseModel):
    """
    Base class for `Project`.

    Attributes:
        name:
        read_only:
    """

    name: str
    read_only: bool = False


class ProjectCreate(_ProjectBase):
    """
    Class for `Project` creation.
    """

    version: str

    # Validators
    _name = validator("name", allow_reuse=True)(valstr("name"))

    @validator("version", always=True, pre=True)
    def validation_version(cls, value):
        if value not in ["v1", "v2"]:
            raise ValueError(f"Allowed versions: 'v1', 'v2'. Given {value}")
        return value


class ProjectRead(_ProjectBase):
    """
    Class for `Project` read from database.

    Attributes:
        id:
        name:
        read_only:
    """

    id: int
    version: str
    timestamp_created: datetime

    _timestamp_created = validator("timestamp_created", allow_reuse=True)(
        valutc("timestamp_created")
    )


class ProjectUpdate(_ProjectBase):
    """
    Class for `Project` update.

    Attributes:
        name:
        read_only:
    """

    name: Optional[str]
    read_only: Optional[bool]

    # Validators
    _name = validator("name", allow_reuse=True)(valstr("name"))
