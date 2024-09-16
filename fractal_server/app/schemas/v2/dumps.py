"""

Dump models differ from their Read counterpart in that:
* They are directly JSON-able, without any additional encoder.
* They may only include a subset of the Read attributes.

These models are used in at least two situations:
1. In the "*_dump" attributes of Job models;
2. In the `_DatasetHistoryItem.workflowtask` model, to trim its size.
"""
from typing import Optional

from pydantic import BaseModel
from pydantic import Extra

from fractal_server.images import Filters


class ProjectDumpV2(BaseModel, extra=Extra.forbid):

    id: int
    name: str
    timestamp_created: str


class TaskDumpV2(BaseModel):
    id: int
    name: str
    type: str

    command_non_parallel: Optional[str]
    command_parallel: Optional[str]
    source: str
    owner: Optional[str]
    version: Optional[str]

    input_types: dict[str, bool]
    output_types: dict[str, bool]


class WorkflowTaskDumpV2(BaseModel):
    id: int
    workflow_id: int
    order: Optional[int]

    input_filters: Filters

    task_id: int
    task: TaskDumpV2


class WorkflowDumpV2(BaseModel, extra=Extra.forbid):
    id: int
    name: str
    project_id: int
    timestamp_created: str


class DatasetDumpV2(BaseModel, extra=Extra.forbid):
    id: int
    name: str
    project_id: int
    timestamp_created: str

    zarr_dir: str
    filters: Filters
