from typing import Any

from pydantic import BaseModel
from pydantic import Field


class Filters(BaseModel):
    attributes: dict[str, Any] = Field(default_factory=dict)
    types: dict[str, bool] = Field(default_factory=dict)
