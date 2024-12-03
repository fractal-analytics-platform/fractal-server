from datetime import datetime
from typing import Any

from pydantic import BaseModel


class StateRead(BaseModel):
    """
    Class for `State` read from database.

    Attributes:
        id:
    """

    id: int | None
    data: dict[str, Any]
    timestamp: datetime
