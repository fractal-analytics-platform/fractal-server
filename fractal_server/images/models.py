from typing import Any
from typing import Optional
from typing import Union

from pydantic import BaseModel
from pydantic import Extra
from pydantic import Field
from pydantic import root_validator
from pydantic import validator

from fractal_server.app.schemas._validators import valdictkeys
from fractal_server.urls import normalize_url


class SingleImageBase(BaseModel):
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
        valdictkeys("attributes")
    )
    _types = validator("types", allow_reuse=True)(valdictkeys("types"))

    @validator("zarr_url")
    def normalize_zarr_url(cls, v: str) -> str:
        return normalize_url(v)

    @validator("origin")
    def normalize_orig(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            return normalize_url(v)


class SingleImageTaskOutput(SingleImageBase):
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


class SingleImage(SingleImageBase):
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
    attributes: Optional[dict[str, Any]]
    types: Optional[dict[str, bool]]

    @validator("zarr_url")
    def normalize_zarr_url(cls, v: str) -> str:
        return normalize_url(v)

    @validator("attributes")
    def validate_attributes(
        cls, v: dict[str, Any]
    ) -> dict[str, Union[int, float, str, bool]]:
        if v is not None:
            # validate keys
            valdictkeys("attributes")(v)
            # validate values
            for key, value in v.items():
                if not isinstance(value, (int, float, str, bool)):
                    raise ValueError(
                        f"SingleImageUpdate.attributes[{key}] must be a scalar"
                        " (int, float, str or bool). "
                        f"Given {value} ({type(value)})"
                    )
        return v

    _types = validator("types", allow_reuse=True)(valdictkeys("types"))


class Filters(BaseModel, extra=Extra.forbid):
    attributes_include: dict[str, list[Any]] = Field(default_factory=dict)
    attributes_exclude: dict[str, list[Any]] = Field(default_factory=dict)
    types: dict[str, bool] = Field(default_factory=dict)

    # Validators
    _attributes_include = validator("attributes_include", allow_reuse=True)(
        valdictkeys("attributes_include")
    )
    _attributes_exclude = validator("attributes_exclude", allow_reuse=True)(
        valdictkeys("attributes_exclude")
    )
    _types = validator("types", allow_reuse=True)(valdictkeys("types"))

    @validator("attributes_include", "attributes_exclude")
    def validate_attributes(
        cls, v: dict[str, list[Any]]
    ) -> dict[str, list[Union[int, float, str, bool, None]]]:
        for key, values in v.items():
            if len(values) == 0:
                raise ValueError(f"Filer {key} ")
            for value in values:
                if not isinstance(value, (int, float, str, bool, type(None))):
                    raise ValueError(
                        "Filter attribute must be scalars "
                        "(int, float, str, bool, or None). "
                        f"For key '{key}' you provided {value} ({type(value)})"
                    )
        return v

    @root_validator
    def validate_attributes_keys(cls, values):

        include = values.get("attributes_include", {})
        exclude = values.get("attributes_exclude", {})
        common_keys = set(include.keys()).intersection(set(exclude.keys()))
        if common_keys:
            raise ValueError(
                "Filters `attributes_include` and `attributes_exclude` should "
                f"have different keys, but they both have '{common_keys}'."
            )
        return values
