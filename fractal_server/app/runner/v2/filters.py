from typing import Any

from pydantic import BaseModel
from pydantic import Field
from pydantic import validator

from fractal_server.images import val_scalar_dict


class Filters(BaseModel):
    attributes: dict[str, Any] = Field(default_factory=dict)
    types: dict[str, bool] = Field(default_factory=dict)

    class Config:
        extra = "forbid"

    # Validators
    _attributes = validator("attributes", allow_reuse=True)(
        val_scalar_dict("attributes")
    )
