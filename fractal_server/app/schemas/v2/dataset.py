from datetime import datetime
from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import Extra
from pydantic import Field
from pydantic import validator

from .._validators import valdict_keys
from .._validators import valdict_scalarvalues
from .._validators import valstr
from .dumps import WorkflowTaskDumpV2
from .project import ProjectReadV2
from .workflowtask import WorkflowTaskStatusTypeV2
from fractal_server.images import SingleImage
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
    attribute_filters: dict[str, list[Any]] = Field(default_factory=dict)

    # Validators

    _name = validator("name", allow_reuse=True)(valstr("name"))

    _type_filters = validator("type_filters", allow_reuse=True)(
        valdict_keys("type_filters")
    )
    _attribute_filters_1 = validator("attribute_filters", allow_reuse=True)(
        valdict_keys("attribute_filters")
    )
    _attribute_filters_2 = validator("attribute_filters", allow_reuse=True)(
        valdict_scalarvalues("attribute_filters")
    )

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
    attribute_filters: dict[str, list[Any]]


class DatasetUpdateV2(BaseModel, extra=Extra.forbid):

    name: Optional[str]
    zarr_dir: Optional[str]
    type_filters: Optional[dict[str, bool]]
    attribute_filters: Optional[dict[str, list[Any]]]

    # Validators

    _name = validator("name", allow_reuse=True)(valstr("name"))

    _type_filters = validator("type_filters", allow_reuse=True)(
        valdict_keys("type_filters")
    )
    _attribute_filters_1 = validator("attribute_filters", allow_reuse=True)(
        valdict_keys("attribute_filters")
    )
    _attribute_filters_2 = validator("attribute_filters", allow_reuse=True)(
        valdict_scalarvalues("attribute_filters")
    )

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
    """

    name: str
    zarr_dir: str
    images: list[SingleImage] = Field(default_factory=list)

    type_filters: dict[str, bool] = Field(default_factory=dict)
    attribute_filters: dict[str, list[Any]] = Field(default_factory=dict)

    # Validators

    _type_filters = validator("type_filters", allow_reuse=True)(
        valdict_keys("type_filters")
    )
    _attribute_filters_1 = validator("attribute_filters", allow_reuse=True)(
        valdict_keys("attribute_filters")
    )
    _attribute_filters_2 = validator("attribute_filters", allow_reuse=True)(
        valdict_scalarvalues("attribute_filters")
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
        filters:
    """

    name: str
    zarr_dir: str
    images: list[SingleImage]
    type_filters: dict[str, bool]
    attribute_filters: dict[str, list[Any]]
