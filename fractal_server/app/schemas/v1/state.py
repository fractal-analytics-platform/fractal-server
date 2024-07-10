from datetime import datetime
from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import validator

from fractal_server.app.schemas._validators import valutc


class StateRead(BaseModel):
    """
    Class for `State` read from database.

    Attributes:
        id:
    """

    id: Optional[int]
    data: dict[str, Any]
    timestamp: datetime

    _timestamp = validator("timestamp", allow_reuse=True)(valutc("timestamp"))
