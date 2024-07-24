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
from pydantic import model_validator

from fractal_server.app.schemas.v1.dumps import TaskDumpV1
from fractal_server.images import Filters


class ProjectDumpV2(BaseModel, extra=Extra.forbid):

    id: int
    name: str
    timestamp_created: str


class TaskDumpV2(BaseModel):
    id: int
    name: str
    type: str

    command_non_parallel: Optional[str] = None
    command_parallel: Optional[str] = None
    source: str
    owner: Optional[str] = None
    version: Optional[str] = None

    input_types: dict[str, bool]
    output_types: dict[str, bool]


class WorkflowTaskDumpV2(BaseModel):
    id: int
    workflow_id: int
    order: Optional[int] = None

    is_legacy_task: bool

    input_filters: Filters

    task_id: Optional[int] = None
    task: Optional[TaskDumpV2] = None
    task_legacy_id: Optional[int] = None
    task_legacy: Optional[TaskDumpV1] = None

    # Validators
    @model_validator(mode="before")
    def task_v1_or_v2(cls, values):
        v1 = values.get("task_legacy_id")
        v2 = values.get("task_id")
        if ((v1 is not None) and (v2 is not None)) or (
            (v1 is None) and (v2 is None)
        ):
            message = "both" if (v1 and v2) else "none"
            raise ValueError(
                "One and only one must be provided between "
                f"'task_legacy_id' and 'task_id' (you provided {message})"
            )
        return values


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
