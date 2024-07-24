from datetime import datetime
from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import field_validator

from fractal_server.app.schemas._validators import valutc


class StateRead(BaseModel):
    """
    Class for `State` read from database.

    Attributes:
        id:
    """

    id: Optional[int] = None
    data: dict[str, Any]
    timestamp: datetime

    _timestamp = field_validator("timestamp")(valutc("timestamp"))
