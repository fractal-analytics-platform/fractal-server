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

from fractal_server.images.models import AttributeFiltersType


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
    source: Optional[str] = None
    version: Optional[str]

    input_types: dict[str, bool]
    output_types: dict[str, bool]


class WorkflowTaskDumpV2(BaseModel):
    """
    We do not include 'extra=Extra.forbid' because legacy data may include
    'input_filters' field and we want to avoid response-validation errors
    for the endpoints that GET datasets.
    """

    id: int
    workflow_id: int
    order: Optional[int]

    type_filters: dict[str, bool]

    task_id: Optional[int]
    task: Optional[TaskDumpV2]


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
    type_filters: dict[str, bool]
    attribute_filters: AttributeFiltersType
