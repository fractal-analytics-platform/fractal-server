"""
Dump models differ from their Read counterpart in that:
* They are directly JSON-able, without any additional encoder.
* They may include only a subset of the available fields.

These models are used in at least two situations:
1. In the "*_dump" attributes of Job models;
2. In the history items, to trim their size.
"""
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field

from .task import TaskTypeType
from .task_group import TaskGroupV2OriginEnum


class ProjectDumpV2(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: int
    name: str
    timestamp_created: str


class TaskDumpV2(BaseModel):
    id: int
    name: str
    type: TaskTypeType

    command_non_parallel: str | None = None
    command_parallel: str | None = None
    source: str | None = None
    version: str | None = None

    input_types: dict[str, bool]
    output_types: dict[str, bool]


class WorkflowTaskDumpV2(BaseModel):
    """
    We do not include 'model_config = ConfigDict(extra="forbid")'
    because legacy data may include 'input_filters' field and we want to avoid
    response-validation errors for the endpoints that GET datasets.
    """

    id: int
    workflow_id: int
    order: int | None = None

    type_filters: dict[str, bool]

    task_id: int | None = None
    task: TaskDumpV2 | None = None


class WorkflowDumpV2(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: int
    name: str
    project_id: int
    timestamp_created: str


class DatasetDumpV2(BaseModel):
    """
    We do not include 'model_config = ConfigDict(extra="forbid")' because
    legacy data may include 'type_filters' or 'attribute_filters' and we
    want to avoid response-validation errors.
    """

    id: int
    name: str
    project_id: int
    timestamp_created: str
    zarr_dir: str


class TaskGroupDumpV2(BaseModel):
    id: int
    origin: TaskGroupV2OriginEnum
    pkg_name: str
    version: str | None = None
    python_version: str | None = None
    pip_extras: str | None = None
    pinned_package_versions: dict[str, str] = Field(default_factory=dict)

    path: str | None = None
    venv_path: str | None = None
    wheel_path: str | None = None
