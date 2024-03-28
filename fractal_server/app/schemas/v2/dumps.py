"""

Dump models differ from their Read counterpart in that:
* They are directly JSON-able, without any additional encoder.
* They may only include a subset of the Read attributes.

These models are used in at least two situations:
1. In the "*_dump" attributes of ApplyWorkflow models;
2. In the `_DatasetHistoryItem.workflowtask` model, to trim its size.
"""
from typing import Optional

from pydantic import BaseModel
from pydantic import Extra
from pydantic import root_validator

from fractal_server.app.schemas.v1.dumps import TaskDumpV1 as TaskDumpV1
from fractal_server.images import Filters
from fractal_server.images import SingleImage


class ProjectDumpV2(BaseModel, extra=Extra.forbid):

    id: int
    name: str
    read_only: bool
    timestamp_created: str


class TaskDumpV2(BaseModel):
    id: int
    name: str

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

    task_id: Optional[int]
    task: Optional[TaskDumpV2]
    task_legacy_id: Optional[int]
    task_legacy: Optional[TaskDumpV1]

    # Validators
    @root_validator
    def task_v1_or_v2(cls, values):
        v1 = values.get("task_v1_id")
        v2 = values.get("task_v2_id")
        if ((v1 is not None) and (v2 is not None)) or (
            (v1 is None) and (v2 is None)
        ):
            message = "both" if (v1 and v2) else "none"
            raise ValueError(
                "One and only one must be provided between "
                f"'task_v1_id' and 'task_v2_id' (you provided {message})"
            )
        return values


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

    zarr_dir: str
    images: list[SingleImage]
    filters: Filters
