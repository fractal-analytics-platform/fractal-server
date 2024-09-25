from datetime import datetime

from pydantic import BaseModel
from pydantic import validator

from .._validators import valutc


__all__ = ("ProjectReadV1",)


class _ProjectBaseV1(BaseModel):
    """
    Base class for `Project`.

    Attributes:
        name:
        read_only:
    """

    name: str
    read_only: bool = False


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

    _timestamp_created = validator("timestamp_created", allow_reuse=True)(
        valutc("timestamp_created")
    )
