from typing import Any

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_validator

from .task import TaskExportV2
from .task import TaskImportV2
from .task import TaskImportV2Legacy
from .task import TaskReadV2
from .task import TaskTypeType
from fractal_server.types import DictStrAny
from fractal_server.types import TypeFilters
from fractal_server.types import WorkflowTaskArgument


class WorkflowTaskCreateV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    meta_non_parallel: DictStrAny | None = None
    meta_parallel: DictStrAny | None = None
    args_non_parallel: WorkflowTaskArgument | None = None
    args_parallel: WorkflowTaskArgument | None = None
    type_filters: TypeFilters = Field(default_factory=dict)


class WorkflowTaskReplaceV2(BaseModel):
    """Used by 'replace-task' endpoint"""

    args_non_parallel: dict[str, Any] | None = None
    args_parallel: dict[str, Any] | None = None


class WorkflowTaskReadV2(BaseModel):
    id: int

    workflow_id: int
    order: int | None = None
    meta_non_parallel: dict[str, Any] | None = None
    meta_parallel: dict[str, Any] | None = None

    args_non_parallel: dict[str, Any] | None = None
    args_parallel: dict[str, Any] | None = None

    type_filters: dict[str, bool]

    task_type: TaskTypeType
    task_id: int
    task: TaskReadV2


class WorkflowTaskReadV2WithWarning(WorkflowTaskReadV2):
    warning: str | None = None


class WorkflowTaskUpdateV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    meta_non_parallel: DictStrAny | None = None
    meta_parallel: DictStrAny | None = None
    args_non_parallel: WorkflowTaskArgument | None = None
    args_parallel: WorkflowTaskArgument | None = None
    type_filters: TypeFilters = None


class WorkflowTaskImportV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    meta_non_parallel: DictStrAny | None = None
    meta_parallel: DictStrAny | None = None
    args_non_parallel: DictStrAny | None = None
    args_parallel: DictStrAny | None = None
    type_filters: TypeFilters | None = None
    input_filters: dict[str, Any] | None = None

    task: TaskImportV2 | TaskImportV2Legacy

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


class WorkflowTaskExportV2(BaseModel):
    meta_non_parallel: dict[str, Any] | None = None
    meta_parallel: dict[str, Any] | None = None
    args_non_parallel: dict[str, Any] | None = None
    args_parallel: dict[str, Any] | None = None
    type_filters: dict[str, bool] = Field(default_factory=dict)

    task: TaskExportV2
