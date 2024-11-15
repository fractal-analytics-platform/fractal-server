from datetime import datetime
from typing import Optional

from pydantic import BaseModel
from pydantic import validator

from .._validators import valstr


__all__ = (
    "ProjectCreateV1",
    "ProjectReadV1",
    "ProjectUpdateV1",
)


class _ProjectBaseV1(BaseModel):
    """
    Base class for `Project`.

    Attributes:
        name:
        read_only:
    """

    name: str
    read_only: bool = False


class ProjectCreateV1(_ProjectBaseV1):
    """
    Class for `Project` creation.
    """

    # Validators
    _name = validator("name", allow_reuse=True)(valstr("name"))


class ProjectReadV1(_ProjectBaseV1):
    """
    Class for `Project` read from database.

    Attributes:
        id:
        name:
        read_only:
    """

    id: int
    timestamp_created: datetime


class ProjectUpdateV1(_ProjectBaseV1):
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
