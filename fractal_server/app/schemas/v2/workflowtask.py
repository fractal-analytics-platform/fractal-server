from enum import Enum
from typing import Any
from typing import Optional
from typing import Union

from pydantic import BaseModel
from pydantic import Extra
from pydantic import Field
from pydantic import root_validator
from pydantic import validator

from .._filter_validators import validate_type_filters
from .._validators import root_validate_dict_keys
from .._validators import valdict_keys
from .task import TaskExportV2
from .task import TaskImportV2
from .task import TaskImportV2Legacy
from .task import TaskReadV2

RESERVED_ARGUMENTS = {"zarr_dir", "zarr_url", "zarr_urls", "init_args"}


class WorkflowTaskStatusTypeV2(str, Enum):
    """
    Define the available values for the status of a `WorkflowTask`.

    This model is used within the `Dataset.history` attribute, which is
    constructed in the runner and then used in the API (e.g. in the
    `api/v2/project/{project_id}/dataset/{dataset_id}/status` endpoint).

    Attributes:
        SUBMITTED: The `WorkflowTask` is part of a running job.
        DONE: The most-recent execution of this `WorkflowTask` was successful.
        FAILED: The most-recent execution of this `WorkflowTask` failed.
    """

    SUBMITTED = "submitted"
    DONE = "done"
    FAILED = "failed"


class WorkflowTaskCreateV2(BaseModel, extra=Extra.forbid):

    meta_non_parallel: Optional[dict[str, Any]]
    meta_parallel: Optional[dict[str, Any]]
    args_non_parallel: Optional[dict[str, Any]]
    args_parallel: Optional[dict[str, Any]]
    type_filters: dict[str, bool] = Field(default_factory=dict)

    # Validators
    _dict_keys = root_validator(pre=True, allow_reuse=True)(
        root_validate_dict_keys
    )
    _type_filters = validator("type_filters", allow_reuse=True)(
        validate_type_filters
    )

    _meta_non_parallel = validator("meta_non_parallel", allow_reuse=True)(
        valdict_keys("meta_non_parallel")
    )
    _meta_parallel = validator("meta_parallel", allow_reuse=True)(
        valdict_keys("meta_parallel")
    )

    @validator("args_non_parallel")
    def validate_args_non_parallel(cls, value):
        if value is None:
            return
        valdict_keys("args_non_parallel")(value)
        args_keys = set(value.keys())
        intersect_keys = RESERVED_ARGUMENTS.intersection(args_keys)
        if intersect_keys:
            raise ValueError(
                "`args` contains the following forbidden keys: "
                f"{intersect_keys}"
            )
        return value

    @validator("args_parallel")
    def validate_args_parallel(cls, value):
        if value is None:
            return
        valdict_keys("args_parallel")(value)
        args_keys = set(value.keys())
        intersect_keys = RESERVED_ARGUMENTS.intersection(args_keys)
        if intersect_keys:
            raise ValueError(
                "`args` contains the following forbidden keys: "
                f"{intersect_keys}"
            )
        return value


class WorkflowTaskReplaceV2(BaseModel):
    """Used by 'replace-task' endpoint"""

    args_non_parallel: Optional[dict[str, Any]] = None
    args_parallel: Optional[dict[str, Any]] = None


class WorkflowTaskReadV2(BaseModel):

    id: int

    workflow_id: int
    order: Optional[int]
    meta_non_parallel: Optional[dict[str, Any]]
    meta_parallel: Optional[dict[str, Any]]

    args_non_parallel: Optional[dict[str, Any]]
    args_parallel: Optional[dict[str, Any]]

    type_filters: dict[str, bool]

    task_type: str
    task_id: int
    task: TaskReadV2


class WorkflowTaskReadV2WithWarning(WorkflowTaskReadV2):
    warning: Optional[str] = None


class WorkflowTaskUpdateV2(BaseModel, extra=Extra.forbid):

    meta_non_parallel: Optional[dict[str, Any]]
    meta_parallel: Optional[dict[str, Any]]
    args_non_parallel: Optional[dict[str, Any]]
    args_parallel: Optional[dict[str, Any]]
    type_filters: Optional[dict[str, bool]]

    # Validators
    _dict_keys = root_validator(pre=True, allow_reuse=True)(
        root_validate_dict_keys
    )
    _type_filters = validator("type_filters", allow_reuse=True)(
        validate_type_filters
    )

    _meta_non_parallel = validator("meta_non_parallel", allow_reuse=True)(
        valdict_keys("meta_non_parallel")
    )
    _meta_parallel = validator("meta_parallel", allow_reuse=True)(
        valdict_keys("meta_parallel")
    )

    @validator("args_non_parallel")
    def validate_args_non_parallel(cls, value):
        if value is None:
            return
        valdict_keys("args_non_parallel")(value)
        args_keys = set(value.keys())
        intersect_keys = RESERVED_ARGUMENTS.intersection(args_keys)
        if intersect_keys:
            raise ValueError(
                "`args` contains the following forbidden keys: "
                f"{intersect_keys}"
            )
        return value

    @validator("args_parallel")
    def validate_args_parallel(cls, value):
        if value is None:
            return
        valdict_keys("args_parallel")(value)
        args_keys = set(value.keys())
        intersect_keys = RESERVED_ARGUMENTS.intersection(args_keys)
        if intersect_keys:
            raise ValueError(
                "`args` contains the following forbidden keys: "
                f"{intersect_keys}"
            )
        return value


class WorkflowTaskImportV2(BaseModel, extra=Extra.forbid):

    meta_non_parallel: Optional[dict[str, Any]] = None
    meta_parallel: Optional[dict[str, Any]] = None
    args_non_parallel: Optional[dict[str, Any]] = None
    args_parallel: Optional[dict[str, Any]] = None
    type_filters: Optional[dict[str, bool]] = None
    filters: Optional[dict[str, Any]] = None

    task: Union[TaskImportV2, TaskImportV2Legacy]

    # Validators
    @root_validator(pre=True)
    def update_legacy_filters(cls, values: dict):
        """
        Transform legacy filters (created with fractal-server<2.11.0)
        into type filters
        """
        if values.get("filters") is not None:
            if "type_filters" in values.keys():
                raise ValueError(
                    "Cannot set filters both through the legacy field "
                    "('filters') and the new one ('type_filters')."
                )

            else:
                # As of 2.11.0, WorkflowTask do not have attribute filters
                # any more.
                if values["filters"]["attributes"] != {}:
                    raise ValueError(
                        "Cannot set attribute filters for WorkflowTasks."
                    )
                # Convert legacy filters.types into new type_filters
                values["type_filters"] = values["filters"].get("types", {})
                values["filters"] = None

        return values

    _type_filters = validator("type_filters", allow_reuse=True)(
        validate_type_filters
    )

    _meta_non_parallel = validator("meta_non_parallel", allow_reuse=True)(
        valdict_keys("meta_non_parallel")
    )
    _meta_parallel = validator("meta_parallel", allow_reuse=True)(
        valdict_keys("meta_parallel")
    )
    _args_non_parallel = validator("args_non_parallel", allow_reuse=True)(
        valdict_keys("args_non_parallel")
    )
    _args_parallel = validator("args_parallel", allow_reuse=True)(
        valdict_keys("args_parallel")
    )


class WorkflowTaskExportV2(BaseModel):

    meta_non_parallel: Optional[dict[str, Any]] = None
    meta_parallel: Optional[dict[str, Any]] = None
    args_non_parallel: Optional[dict[str, Any]] = None
    args_parallel: Optional[dict[str, Any]] = None
    type_filters: dict[str, bool] = Field(default_factory=dict)

    task: TaskExportV2
