from typing import Any
from typing import Optional
from typing import Union

from pydantic import BaseModel
from pydantic import Field
from pydantic import validator


class SingleImage(BaseModel):

    path: str
    origin: Optional[str] = None

    attributes: dict[str, Any] = Field(default_factory=dict)
    types: dict[str, bool] = Field(default_factory=dict)

    @validator("attributes")
    def validate_attributes(
        cls, v: dict[str, Any]
    ) -> dict[str, Union[int, float, str, bool]]:
        for key, value in v.items():
            if not isinstance(value, (int, float, str, bool)):
                raise ValueError(
                    f"SingleImage.attributes[{key}] must be a scalar "
                    f"(int, float, str or bool). Given {value} ({type(value)})"
                )
        return v


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
            if not isinstance(value, (int, float, str, bool, type(None))):
                raise ValueError(
                    f"Filters.attributes[{key}] must be a scalar "
                    "(int, float, str, bool, or None). "
                    f"Given {value} ({type(value)})"
                )
        return v
