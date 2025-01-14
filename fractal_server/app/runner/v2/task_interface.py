from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import Extra
from pydantic import Field
from pydantic import root_validator
from pydantic import validator

from ....images import SingleImageTaskOutput
from fractal_server.app.schemas._filter_validators import validate_type_filters
from fractal_server.app.schemas._validators import root_validate_dict_keys
from fractal_server.urls import normalize_url


class LegacyFilters(BaseModel, extra=Extra.forbid):
    """
    For fractal-server<2.11, task output could include both
    `filters["attributes"]` and `filters["types"]`. In the new version
    there is a single field, named `type_filters`.
    The current schema is only used to convert old type filters into the
    new form, but it will reject any attribute filters.
    """

    types: dict[str, bool] = Field(default_factory=dict)
    _types = validator("types", allow_reuse=True)(validate_type_filters)


class TaskOutput(BaseModel, extra=Extra.forbid):

    image_list_updates: list[SingleImageTaskOutput] = Field(
        default_factory=list
    )
    image_list_removals: list[str] = Field(default_factory=list)

    filters: Optional[LegacyFilters] = None
    type_filters: dict[str, bool] = Field(default_factory=dict)

    _dict_keys = root_validator(pre=True, allow_reuse=True)(
        root_validate_dict_keys
    )
    _type_filters = validator("type_filters", allow_reuse=True)(
        validate_type_filters
    )

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
    def update_legacy_filters(cls, values):
        if values["filters"] is not None:
            if values["type_filters"] != {}:
                raise ValueError(
                    "Cannot set both (legacy) 'filters' and 'type_filters'."
                )
            else:
                # Convert legacy filters.types into new type_filters
                values["type_filters"] = values["filters"].types
                values["filters"] = None

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
