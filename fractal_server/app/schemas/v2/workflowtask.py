from enum import Enum
from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator

from .._validators import valdictkeys
from .._validators import valint
from ..v1.task import TaskExportV1
from ..v1.task import TaskImportV1
from .task import TaskExportV2
from .task import TaskImportV2
from .task import TaskLegacyReadV2
from .task import TaskReadV2
from fractal_server.images import Filters

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


class WorkflowTaskCreateV2(BaseModel):

    model_config = ConfigDict(extra="forbid")

    meta_non_parallel: Optional[dict[str, Any]] = None
    meta_parallel: Optional[dict[str, Any]] = None
    args_non_parallel: Optional[dict[str, Any]] = None
    args_parallel: Optional[dict[str, Any]] = None
    order: Optional[int] = None
    input_filters: Filters = Field(default_factory=Filters)

    is_legacy_task: bool = False

    # Validators
    _meta_non_parallel = field_validator("meta_non_parallel")(
        valdictkeys("meta_non_parallel")
    )
    _meta_parallel = field_validator("meta_parallel")(
        valdictkeys("meta_parallel")
    )
    _order = field_validator("order")(valint("order", min_val=0))

    @field_validator("args_non_parallel")
    @classmethod
    def validate_args_non_parallel(cls, value):
        if value is None:
            return
        valdictkeys("args_non_parallel")(value)
        args_keys = set(value.keys())
        intersect_keys = RESERVED_ARGUMENTS.intersection(args_keys)
        if intersect_keys:
            raise ValueError(
                "`args` contains the following forbidden keys: "
                f"{intersect_keys}"
            )
        return value

    @field_validator("args_parallel")
    @classmethod
    def validate_args_parallel(cls, value):
        if value is None:
            return
        valdictkeys("args_parallel")(value)
        args_keys = set(value.keys())
        intersect_keys = RESERVED_ARGUMENTS.intersection(args_keys)
        if intersect_keys:
            raise ValueError(
                "`args` contains the following forbidden keys: "
                f"{intersect_keys}"
            )
        return value

    @model_validator(mode="before")
    def validate_legacy_task(cls, values):
        if values["is_legacy_task"] and (
            values.get("meta_non_parallel") is not None
            or values.get("args_non_parallel") is not None
        ):
            raise ValueError(
                "If Task is legacy, 'args_non_parallel' and 'meta_non_parallel"
                "must be None"
            )
        return values


class WorkflowTaskReadV2(BaseModel):

    id: int

    workflow_id: int
    order: Optional[int] = None
    meta_non_parallel: Optional[dict[str, Any]] = None
    meta_parallel: Optional[dict[str, Any]] = None

    args_non_parallel: Optional[dict[str, Any]] = None
    args_parallel: Optional[dict[str, Any]] = None

    input_filters: Filters

    is_legacy_task: bool
    task_type: str
    task_id: Optional[int] = None
    task: Optional[TaskReadV2] = None
    task_legacy_id: Optional[int] = None
    task_legacy: Optional[TaskLegacyReadV2] = None


class WorkflowTaskUpdateV2(BaseModel):

    meta_non_parallel: Optional[dict[str, Any]] = None
    meta_parallel: Optional[dict[str, Any]] = None
    args_non_parallel: Optional[dict[str, Any]] = None
    args_parallel: Optional[dict[str, Any]] = None
    input_filters: Optional[Filters] = None

    # Validators
    _meta_non_parallel = field_validator("meta_non_parallel")(
        valdictkeys("meta_non_parallel")
    )
    _meta_parallel = field_validator("meta_parallel")(
        valdictkeys("meta_parallel")
    )

    @field_validator("args_non_parallel")
    @classmethod
    def validate_args_non_parallel(cls, value):
        if value is None:
            return
        valdictkeys("args_non_parallel")(value)
        args_keys = set(value.keys())
        intersect_keys = RESERVED_ARGUMENTS.intersection(args_keys)
        if intersect_keys:
            raise ValueError(
                "`args` contains the following forbidden keys: "
                f"{intersect_keys}"
            )
        return value

    @field_validator("args_parallel")
    @classmethod
    def validate_args_parallel(cls, value):
        if value is None:
            return
        valdictkeys("args_parallel")(value)
        args_keys = set(value.keys())
        intersect_keys = RESERVED_ARGUMENTS.intersection(args_keys)
        if intersect_keys:
            raise ValueError(
                "`args` contains the following forbidden keys: "
                f"{intersect_keys}"
            )
        return value


class WorkflowTaskImportV2(BaseModel):

    meta_non_parallel: Optional[dict[str, Any]] = None
    meta_parallel: Optional[dict[str, Any]] = None
    args_non_parallel: Optional[dict[str, Any]] = None
    args_parallel: Optional[dict[str, Any]] = None

    input_filters: Optional[Filters] = None

    is_legacy_task: bool = False
    task: Optional[TaskImportV2] = None
    task_legacy: Optional[TaskImportV1] = None

    _meta_non_parallel = field_validator("meta_non_parallel")(
        valdictkeys("meta_non_parallel")
    )
    _meta_parallel = field_validator("meta_parallel")(
        valdictkeys("meta_parallel")
    )
    _args_non_parallel = field_validator("args_non_parallel")(
        valdictkeys("args_non_parallel")
    )
    _args_parallel = field_validator("args_parallel")(
        valdictkeys("args_parallel")
    )


class WorkflowTaskExportV2(BaseModel):

    meta_non_parallel: Optional[dict[str, Any]] = None
    meta_parallel: Optional[dict[str, Any]] = None
    args_non_parallel: Optional[dict[str, Any]] = None
    args_parallel: Optional[dict[str, Any]] = None
    input_filters: Filters = Field(default_factory=Filters)

    is_legacy_task: bool = False
    task: Optional[TaskExportV2] = None
    task_legacy: Optional[TaskExportV1] = None
