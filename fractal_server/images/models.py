from typing import Any
from typing import Optional
from typing import Union

from pydantic import BaseModel
from pydantic import Field
from pydantic import validator

from fractal_server.app.schemas._validators import valdict_keys
from fractal_server.urls import normalize_url

AttributeFiltersType = dict[str, list[Any]]


class _SingleImageBase(BaseModel):
    """
    Base for SingleImage and SingleImageTaskOutput.

    Attributes:
        zarr_url:
        origin:
        attributes:
        types:
    """

    zarr_url: str
    origin: Optional[str] = None

    attributes: dict[str, Any] = Field(default_factory=dict)
    types: dict[str, bool] = Field(default_factory=dict)

    # Validators
    _attributes = validator("attributes", allow_reuse=True)(
        valdict_keys("attributes")
    )
    _types = validator("types", allow_reuse=True)(valdict_keys("types"))

    @validator("zarr_url")
    def normalize_zarr_url(cls, v: str) -> str:
        return normalize_url(v)

    @validator("origin")
    def normalize_orig(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            return normalize_url(v)


class SingleImageTaskOutput(_SingleImageBase):
    """
    `SingleImageBase`, with scalar `attributes` values (`None` included).
    """

    @validator("attributes")
    def validate_attributes(
        cls, v: dict[str, Any]
    ) -> dict[str, Union[int, float, str, bool, None]]:
        for key, value in v.items():
            if not isinstance(value, (int, float, str, bool, type(None))):
                raise ValueError(
                    f"SingleImageTaskOutput.attributes[{key}] must be a "
                    "scalar (int, float, str or bool). "
                    f"Given {value} ({type(value)})"
                )
        return v


class SingleImage(_SingleImageBase):
    """
    `SingleImageBase`, with scalar `attributes` values (`None` excluded).
    """

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


class SingleImageUpdate(BaseModel):
    zarr_url: str
    attributes: Optional[dict[str, Any]] = None
    types: Optional[dict[str, bool]] = None

    @validator("zarr_url")
    def normalize_zarr_url(cls, v: str) -> str:
        return normalize_url(v)

    @validator("attributes")
    def validate_attributes(
        cls, v: dict[str, Any]
    ) -> dict[str, Union[int, float, str, bool]]:
        if v is not None:
            # validate keys
            valdict_keys("attributes")(v)
            # validate values
            for key, value in v.items():
                if not isinstance(value, (int, float, str, bool)):
                    raise ValueError(
                        f"SingleImageUpdate.attributes[{key}] must be a scalar"
                        " (int, float, str or bool). "
                        f"Given {value} ({type(value)})"
                    )
        return v

    _types = validator("types", allow_reuse=True)(valdict_keys("types"))
