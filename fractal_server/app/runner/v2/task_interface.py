from typing import Any
from typing import Optional
from typing import Union

from pydantic import BaseModel
from pydantic import Extra
from pydantic import Field
from pydantic import root_validator
from pydantic import validator

from ....images import SingleImageTaskOutput
from fractal_server.app.schemas._validators import valdict_keys
from fractal_server.urls import normalize_url


class LegacyFilters(BaseModel, extra=Extra.forbid):
    attributes: dict[str, Any] = Field(default_factory=dict)
    types: dict[str, bool] = Field(default_factory=dict)
    # Validators
    _attributes = validator("attributes", allow_reuse=True)(
        valdict_keys("attributes")
    )
    _types = validator("types", allow_reuse=True)(valdict_keys("types"))

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


class TaskOutput(BaseModel, extra=Extra.forbid):

    image_list_updates: list[SingleImageTaskOutput] = Field(
        default_factory=list
    )
    image_list_removals: list[str] = Field(default_factory=list)

    filters: Optional[LegacyFilters] = None
    type_filters: Optional[dict[str, bool]] = None

    def check_zarr_urls_are_unique(self) -> None:
        zarr_urls = [img.zarr_url for img in self.image_list_updates]
        zarr_urls.extend(self.image_list_removals)
        if len(zarr_urls) != len(set(zarr_urls)):
            duplicates = [
                zarr_url
                for zarr_url in set(zarr_urls)
                if zarr_urls.count(zarr_url) > 1
            ]
            msg = (
                "TaskOutput "
                f"({len(self.image_list_updates)} image_list_updates and "
                f"{len(self.image_list_removals)} image_list_removals) "
                "has non-unique zarr_urls:"
            )
            for duplicate in duplicates:
                msg = f"{msg}\n{duplicate}"
            raise ValueError(msg)

    @root_validator()
    def validate_filters(cls, values):
        if values["filters"] is not None:
            if values["type_filters"] is not None:
                raise ValueError(
                    "Cannot set both (legacy) 'filters' and 'type_filters'."
                )
            elif values["filters"].attributes:
                raise ValueError(
                    "Legacy 'filters' cannot contain 'attributes'."
                )
            else:
                values["type_filters"] = values["filters"].types
        return values

    @validator("image_list_removals")
    def normalize_paths(cls, v: list[str]) -> list[str]:
        return [normalize_url(zarr_url) for zarr_url in v]


class InitArgsModel(BaseModel, extra=Extra.forbid):

    zarr_url: str
    init_args: dict[str, Any] = Field(default_factory=dict)

    @validator("zarr_url")
    def normalize_path(cls, v: str) -> str:
        return normalize_url(v)


class InitTaskOutput(BaseModel, extra=Extra.forbid):

    parallelization_list: list[InitArgsModel] = Field(default_factory=list)
