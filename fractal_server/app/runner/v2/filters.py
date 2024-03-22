from typing import Any
from typing import Union

from pydantic import BaseModel
from pydantic import Field
from pydantic import validator


class Filters(BaseModel):
    attributes: dict[str, Any] = Field(default_factory=dict)
    types: dict[str, bool] = Field(default_factory=dict)

    class Config:
        extra = "forbid"

    # Validators
    @validator("attributes")
    def validate_attributes(
        cls, v: dict[str, Any]
    ) -> dict[str, Union[int, float, str, bool, None]]:
        for key, value in v.items():
            if not isinstance(value, (int, float, str, bool, None)):
                raise ValueError(
                    f"Filters.attributes[{key}] must be a scalar "
                    "(int, float, str, bool, or None). "
                    f"Given {value} ({type(value)})"
                )
        return v
