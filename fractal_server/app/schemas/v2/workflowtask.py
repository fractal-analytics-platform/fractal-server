from typing import Any

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_validator

from fractal_server.types import DictStrAny
from fractal_server.types import TypeFilters
from fractal_server.types import WorkflowTaskArgument

from .task import TaskExport
from .task import TaskImport
from .task import TaskImportLegacy
from .task import TaskRead
from .task import TaskType


class WorkflowTaskCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    meta_non_parallel: DictStrAny | None = None
    meta_parallel: DictStrAny | None = None
    args_non_parallel: WorkflowTaskArgument | None = None
    args_parallel: WorkflowTaskArgument | None = None
    type_filters: TypeFilters = Field(default_factory=dict)


class WorkflowTaskReplace(BaseModel):
    """Used by 'replace-task' endpoint"""

    args_non_parallel: dict[str, Any] | None = None
    args_parallel: dict[str, Any] | None = None


class WorkflowTaskRead(BaseModel):
    id: int

    workflow_id: int
    order: int | None = None
    meta_non_parallel: dict[str, Any] | None = None
    meta_parallel: dict[str, Any] | None = None

    args_non_parallel: dict[str, Any] | None = None
    args_parallel: dict[str, Any] | None = None

    type_filters: dict[str, bool]

    task_type: TaskType
    task_id: int
    task: TaskRead


class WorkflowTaskReadWithWarning(WorkflowTaskRead):
    warning: str | None = None


class WorkflowTaskUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    meta_non_parallel: DictStrAny | None = None
    meta_parallel: DictStrAny | None = None
    args_non_parallel: WorkflowTaskArgument | None = None
    args_parallel: WorkflowTaskArgument | None = None
    type_filters: TypeFilters = None


class WorkflowTaskImport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    meta_non_parallel: DictStrAny | None = None
    meta_parallel: DictStrAny | None = None
    args_non_parallel: DictStrAny | None = None
    args_parallel: DictStrAny | None = None
    type_filters: TypeFilters | None = None
    input_filters: dict[str, Any] | None = None

    task: TaskImport | TaskImportLegacy

    @model_validator(mode="before")
    @classmethod
    def update_legacy_filters(cls, values: dict):
        """
        Transform legacy filters (created with fractal-server<2.11.0)
        into type filters
        """
        if values.get("input_filters") is not None:
            if "type_filters" in values.keys():
                raise ValueError(
                    "Cannot set filters both through the legacy field "
                    "('filters') and the new one ('type_filters')."
                )
            else:
                # As of 2.11.0, WorkflowTask do not have attribute filters
                # any more.
                if values["input_filters"]["attributes"] != {}:
                    raise ValueError(
                        "Cannot set attribute filters for WorkflowTasks."
                    )
                # Convert legacy filters.types into new type_filters
                values["type_filters"] = values["input_filters"].get(
                    "types", {}
                )
                values["input_filters"] = None

        return values


class WorkflowTaskExport(BaseModel):
    meta_non_parallel: dict[str, Any] | None = None
    meta_parallel: dict[str, Any] | None = None
    args_non_parallel: dict[str, Any] | None = None
    args_parallel: dict[str, Any] | None = None
    type_filters: dict[str, bool] = Field(default_factory=dict)

    task: TaskExport
