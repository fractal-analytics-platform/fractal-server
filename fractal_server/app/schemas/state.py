from datetime import datetime
from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import validator

from ._validators import valutc

__all__ = (
    "_StateBase",
    "StateRead",
)


class _StateBase(BaseModel):
    """
    Base class for `State`.

    Attributes:
        id: Primary key
        data: Content of the state
        timestamp: Time stamp of the state
    """

    data: dict[str, Any]
    timestamp: datetime


class StateRead(_StateBase):
    """
    Class for `State` read from database.

    Attributes:
        id:
    """

    id: Optional[int]

    _timestamp = validator("timestamp", allow_reuse=True)(valutc("timestamp"))
