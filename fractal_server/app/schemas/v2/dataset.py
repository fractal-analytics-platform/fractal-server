from datetime import datetime
from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import validator

from ....images import val_scalar_dict
from .._validators import valstr
from .._validators import valutc
from ..v1.project import ProjectRead
from .dumps import WorkflowTaskDumpV2
from .workflowtask import WorkflowTaskStatusTypeV2


class _DatasetHistoryItemV2(BaseModel):
    """
    Class for an item of `Dataset.history`.
    """

    workflowtask: WorkflowTaskDumpV2
    status: WorkflowTaskStatusTypeV2
    parallelization: Optional[dict]


class DatasetStatusReadV2(BaseModel):
    """
    Response type for the
    `/project/{project_id}/dataset/{dataset_id}/status/` endpoint
    """

    status: Optional[
        dict[
            int,
            WorkflowTaskStatusTypeV2,
        ]
    ] = None


# CRUD


class DatasetCreateV2(BaseModel):

    name: str

    read_only: bool = False
    zarr_dir: str

    attribute_filters: dict[str, Any] = {}
    flag_filters: dict[str, bool] = {}

    # Validators
    _name = validator("name", allow_reuse=True)(valstr("name"))
    _attribute_filters = validator("attribute_filters", allow_reuse=True)(
        val_scalar_dict("attribute_filters")
    )


class DatasetReadV2(BaseModel):

    id: int
    name: str

    project_id: int
    project: ProjectRead

    history: list[_DatasetHistoryItemV2]
    read_only: bool

    timestamp_created: datetime

    zarr_dir: str
    attribute_filters: dict[str, Any] = {}
    flag_filters: dict[str, bool] = {}

    # Validators
    _timestamp_created = validator("timestamp_created", allow_reuse=True)(
        valutc("timestamp_created")
    )
    _attribute_filters = validator("attribute_filters", allow_reuse=True)(
        val_scalar_dict("attribute_filters")
    )


class DatasetUpdateV2(BaseModel):
    class Config:
        extra = "forbid"

    name: Optional[str]
    read_only: Optional[bool]
    zarr_dir: Optional[str]
    attribute_filters: Optional[dict[str, Any]]
    flag_filters: Optional[dict[str, bool]]

    # Validators
    _name = validator("name", allow_reuse=True)(valstr("name"))
    _attribute_filters = validator("attribute_filters", allow_reuse=True)(
        val_scalar_dict("attribute_filters")
    )
