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


class ProjectDump(BaseModel, extra=Extra.forbid):

    id: int
    name: str
    read_only: bool
    timestamp_created: str


class TaskDump(BaseModel):
    id: int
    source: str
    name: str
    command: str
    input_type: str
    output_type: str
    owner: Optional[str]
    version: Optional[str]


class WorkflowTaskDump(BaseModel):
    id: int
    order: Optional[int]
    workflow_id: int
    task_id: int
    task: TaskDump


class WorkflowDump(BaseModel):
    id: int
    name: str
    project_id: int
    task_list: list[WorkflowTaskDump]
    timestamp_created: str


class ResourceDump(BaseModel):
    id: int
    path: str
    dataset_id: int


class DatasetDump(BaseModel):
    id: int
    name: str
    type: Optional[str]
    read_only: bool
    resource_list: list[ResourceDump]
    project_id: int
    timestamp_created: str
