from typing import Any
from typing import Optional
from typing import Union

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_validator

from .._validated_types import DictStrBool
from .._validated_types import OptionalDictStrAny
from .._validated_types import WorkflowTaskArgument
from .._validators import root_validate_dict_keys
from .task import TaskExportV2
from .task import TaskImportV2
from .task import TaskImportV2Legacy
from .task import TaskReadV2
from .task import TaskTypeType


class WorkflowTaskCreateV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    meta_non_parallel: OptionalDictStrAny = None
    meta_parallel: OptionalDictStrAny = None
    args_non_parallel: Optional[WorkflowTaskArgument] = None
    args_parallel: Optional[WorkflowTaskArgument] = None
    type_filters: DictStrBool = Field(default_factory=dict)

    _dict_keys = model_validator(mode="before")(root_validate_dict_keys)


class WorkflowTaskReplaceV2(BaseModel):
    """Used by 'replace-task' endpoint"""

    args_non_parallel: Optional[dict[str, Any]] = None
    args_parallel: Optional[dict[str, Any]] = None


class WorkflowTaskReadV2(BaseModel):
    id: int

    workflow_id: int
    order: Optional[int] = None
    meta_non_parallel: Optional[dict[str, Any]] = None
    meta_parallel: Optional[dict[str, Any]] = None

    args_non_parallel: Optional[dict[str, Any]] = None
    args_parallel: Optional[dict[str, Any]] = None

    type_filters: dict[str, bool]

    task_type: TaskTypeType
    task_id: int
    task: TaskReadV2


class WorkflowTaskReadV2WithWarning(WorkflowTaskReadV2):
    warning: Optional[str] = None


class WorkflowTaskUpdateV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    meta_non_parallel: OptionalDictStrAny = None
    meta_parallel: OptionalDictStrAny = None
    args_non_parallel: Optional[WorkflowTaskArgument] = None
    args_parallel: Optional[WorkflowTaskArgument] = None
    type_filters: DictStrBool = None

    _dict_keys = model_validator(mode="before")(root_validate_dict_keys)


class WorkflowTaskImportV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    meta_non_parallel: OptionalDictStrAny = None
    meta_parallel: OptionalDictStrAny = None
    args_non_parallel: OptionalDictStrAny = None
    args_parallel: OptionalDictStrAny = None
    type_filters: DictStrBool = None
    input_filters: Optional[dict[str, Any]] = None

    task: Union[TaskImportV2, TaskImportV2Legacy]

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
    meta_non_parallel: Optional[dict[str, Any]] = None
    meta_parallel: Optional[dict[str, Any]] = None
    args_non_parallel: Optional[dict[str, Any]] = None
    args_parallel: Optional[dict[str, Any]] = None
    type_filters: dict[str, bool] = Field(default_factory=dict)

    task: TaskExportV2
