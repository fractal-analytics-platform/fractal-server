from datetime import datetime
from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import Extra
from pydantic import Field
from pydantic import root_validator
from pydantic import validator

from .._filter_validators import validate_attribute_filters
from .._filter_validators import validate_type_filters
from .._validators import root_validate_dict_keys
from .._validators import valstr
from .dumps import WorkflowTaskDumpV2
from .project import ProjectReadV2
from .workflowtask import WorkflowTaskStatusTypeV2
from fractal_server.images import SingleImage
from fractal_server.images.models import AttributeFiltersType
from fractal_server.urls import normalize_url


class _DatasetHistoryItemV2(BaseModel):
    """
    Class for an item of `Dataset.history`.
    """

    workflowtask: WorkflowTaskDumpV2
    status: WorkflowTaskStatusTypeV2
    parallelization: Optional[dict]


# CRUD


class DatasetCreateV2(BaseModel, extra=Extra.forbid):

    name: str

    zarr_dir: Optional[str] = None

    type_filters: dict[str, bool] = Field(default_factory=dict)
    attribute_filters: AttributeFiltersType = Field(default_factory=dict)

    # Validators

    _dict_keys = root_validator(pre=True, allow_reuse=True)(
        root_validate_dict_keys
    )
    _type_filters = validator("type_filters", allow_reuse=True)(
        validate_type_filters
    )
    _attribute_filters = validator("attribute_filters", allow_reuse=True)(
        validate_attribute_filters
    )

    _name = validator("name", allow_reuse=True)(valstr("name"))

    @validator("zarr_dir")
    def normalize_zarr_dir(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            return normalize_url(v)
        return v


class DatasetReadV2(BaseModel):

    id: int
    name: str

    project_id: int
    project: ProjectReadV2

    history: list[_DatasetHistoryItemV2]

    timestamp_created: datetime

    zarr_dir: str
    type_filters: dict[str, bool]
    attribute_filters: AttributeFiltersType


class DatasetUpdateV2(BaseModel, extra=Extra.forbid):

    name: Optional[str]
    zarr_dir: Optional[str]
    type_filters: Optional[dict[str, bool]]
    attribute_filters: Optional[dict[str, list[Any]]]

    # Validators

    _dict_keys = root_validator(pre=True, allow_reuse=True)(
        root_validate_dict_keys
    )
    _type_filters = validator("type_filters", allow_reuse=True)(
        validate_type_filters
    )
    _attribute_filters = validator("attribute_filters", allow_reuse=True)(
        validate_attribute_filters
    )

    _name = validator("name", allow_reuse=True)(valstr("name"))

    @validator("zarr_dir")
    def normalize_zarr_dir(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            return normalize_url(v)
        return v


class DatasetImportV2(BaseModel, extra=Extra.forbid):
    """
    Class for `Dataset` import.

    Attributes:
        name:
        zarr_dir:
        images:
        filters:
        type_filters:
        attribute_filters:
    """

    name: str
    zarr_dir: str
    images: list[SingleImage] = Field(default_factory=list)

    filters: Optional[dict[str, Any]] = None
    type_filters: dict[str, bool] = Field(default_factory=dict)
    attribute_filters: AttributeFiltersType = Field(default_factory=dict)

    @root_validator(pre=True)
    def update_legacy_filters(cls, values: dict):
        """
        Transform legacy filters (created with fractal-server<2.11.0)
        into attribute/type filters
        """
        if values.get("filters") is not None:
            if (
                "type_filters" in values.keys()
                or "attribute_filters" in values.keys()
            ):
                raise ValueError(
                    "Cannot set filters both through the legacy field "
                    "('filters') and the new ones ('type_filters' and/or "
                    "'attribute_filters')."
                )

            else:
                # Convert legacy filters.types into new type_filters
                values["type_filters"] = values["filters"].get("types", {})
                values["attribute_filters"] = {
                    key: [value]
                    for key, value in values["filters"]
                    .get("attributes", {})
                    .items()
                }
                values["filters"] = None

        return values

    _type_filters = validator("type_filters", allow_reuse=True)(
        validate_type_filters
    )
    _attribute_filters = validator("attribute_filters", allow_reuse=True)(
        validate_attribute_filters
    )

    @validator("zarr_dir")
    def normalize_zarr_dir(cls, v: str) -> str:
        return normalize_url(v)


class DatasetExportV2(BaseModel):
    """
    Class for `Dataset` export.

    Attributes:
        name:
        zarr_dir:
        images:
        type_filters:
        attribute_filters:
    """

    name: str
    zarr_dir: str
    images: list[SingleImage]
    type_filters: dict[str, bool]
    attribute_filters: AttributeFiltersType
