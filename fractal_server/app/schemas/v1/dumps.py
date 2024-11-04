"""

Dump models differ from their Read counterpart in that:
* They are directly JSON-able, without any additional encoder.
* They may only include a subset of the Read attributes.

These models are used in at least two situations:
1. In the "*_dump" attributes of ApplyWorkflow models;
2. In the `_DatasetHistoryItem.workflowtask` model, to trim its size.
"""
from pydantic import BaseModel
from pydantic import Extra


class ProjectDumpV1(BaseModel, extra=Extra.forbid):

    id: int
    name: str
    read_only: bool
    timestamp_created: str


class TaskDumpV1(BaseModel):
    id: int
    source: str
    name: str
    command: str
    input_type: str
    output_type: str
    owner: str | None
    version: str | None


class WorkflowTaskDumpV1(BaseModel):
    id: int
    order: int | None
    workflow_id: int
    task_id: int
    task: TaskDumpV1


class WorkflowDumpV1(BaseModel):
    id: int
    name: str
    project_id: int
    timestamp_created: str


class ResourceDumpV1(BaseModel):
    id: int
    path: str
    dataset_id: int


class DatasetDumpV1(BaseModel):
    id: int
    name: str
    type: str | None
    read_only: bool
    resource_list: list[ResourceDumpV1]
    project_id: int
    timestamp_created: str
