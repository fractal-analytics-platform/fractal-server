"""

Dump models differ from their Read counterpart in that:
* They are directly JSON-able, without any additional encoder.
* They may only include a subset of the Read attributes.

These models are used in at least two situations:
1. In the "*_dump" attributes of ApplyWorkflow models;
2. In the `_DatasetHistoryItem.workflowtask` model, to trim its size.
"""
from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import Extra
from pydantic import validator

from ._validators_v2 import val_scalar_dict
from .image import SingleImage


class ProjectDumpV2(BaseModel, extra=Extra.forbid):

    id: int
    version: str  # ! new
    name: str
    read_only: bool
    timestamp_created: str


class TaskDumpV2(BaseModel):
    id: int
    name: str
    is_parallel: bool  # ! new
    source: str
    command: str
    owner: Optional[str]
    version: Optional[str]


class WorkflowTaskDumpV2(BaseModel):
    id: int
    order: Optional[int]
    workflow_id: int
    task_id: int
    task: TaskDumpV2
    filters: dict[str, Any]  # ! new
    _filters = validator("filters", allow_reuse=True)(
        val_scalar_dict("filters")
    )


class WorkflowDumpV2(BaseModel):
    id: int
    name: str
    project_id: int
    timestamp_created: str


class DatasetDumpV2(BaseModel):
    id: int
    name: str
    project_id: int
    read_only: bool
    timestamp_created: str

    images: list[SingleImage]  # ! new
    filters: dict[str, Any]  # ! new
    _filters = validator("filters", allow_reuse=True)(
        val_scalar_dict("filters")
    )
