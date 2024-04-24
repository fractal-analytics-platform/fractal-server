from datetime import datetime
from typing import Optional

from pydantic import BaseModel
from pydantic import Extra
from pydantic import Field
from pydantic import validator

from .._validators import valstr
from .._validators import valutc
from .dumps import WorkflowTaskDumpV2
from .project import ProjectReadV2
from .workflowtask import WorkflowTaskStatusTypeV2
from fractal_server.images import Filters
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

    zarr_dir: str

    filters: Filters = Field(default_factory=Filters)

    # Validators
    @validator("zarr_dir")
    def normalize_zarr_dir(cls, v: str) -> str:
        return normalize_url(v)

    _name = validator("name", allow_reuse=True)(valstr("name"))


class DatasetReadV2(BaseModel):

    id: int
    name: str

    project_id: int
    project: ProjectReadV2

    history: list[_DatasetHistoryItemV2]

    timestamp_created: datetime

    zarr_dir: str
    filters: Filters = Field(default_factory=Filters)

    # Validators
    _timestamp_created = validator("timestamp_created", allow_reuse=True)(
        valutc("timestamp_created")
    )


class DatasetUpdateV2(BaseModel):
    class Config:
        extra = "forbid"

    name: Optional[str]
    zarr_dir: Optional[str]
    filters: Optional[Filters]

    # Validators
    @validator("zarr_dir")
    def normalize_zarr_dir(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            return normalize_url(v)
        return v

    _name = validator("name", allow_reuse=True)(valstr("name"))


class DatasetImportV2(BaseModel):
    """
    Class for `Dataset` import.

    Attributes:
        name:
        zarr_dir:
        images:
        filters:
    """

    class Config:
        extra = "forbid"

    name: str
    zarr_dir: str
    images: list[SingleImage] = Field(default_factory=[])
    filters: Filters = Field(default_factory=Filters)

    # Validators
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
    filters: Filters
