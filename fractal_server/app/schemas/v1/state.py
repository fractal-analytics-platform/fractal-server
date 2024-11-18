from datetime import datetime
from typing import Any
from typing import Optional

from pydantic import BaseModel


class StateRead(BaseModel):
    """
    Class for `State` read from database.

    Attributes:
        id:
    """

    id: Optional[int]
    data: dict[str, Any]
    timestamp: datetime
