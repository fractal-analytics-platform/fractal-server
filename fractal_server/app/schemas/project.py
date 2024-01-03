from datetime import datetime
from typing import Optional

from pydantic import BaseModel
from pydantic import field_validator

from ._validators import valstr


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

    # Validators
    _name = field_validator("name")(valstr("name"))


class ProjectRead(_ProjectBase):
    """
    Class for `Project` read from database.

    Attributes:
        id:
        name:
        read_only:
    """

    id: int
    timestamp_created: datetime


class ProjectUpdate(_ProjectBase):
    """
    Class for `Project` update.

    Attributes:
        name:
        read_only:
    """

    name: Optional[str] = None
    read_only: Optional[bool] = None

    # Validators
    _name = field_validator("name")(valstr("name"))
