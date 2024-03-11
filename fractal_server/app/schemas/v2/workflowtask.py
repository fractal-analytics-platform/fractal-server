from enum import Enum
from typing import Any
from typing import Optional
from typing import Union

from pydantic import BaseModel
from pydantic import root_validator
from pydantic import validator

from .._validators import valint
from ..v1.task import TaskRead
from .task import TaskExportV2
from .task import TaskImportV2
from .task import TaskReadV2
from fractal_server.images import val_scalar_dict


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

    meta: Optional[dict[str, Any]]
    args: Optional[dict[str, Any]]
    order: Optional[int]
    filters: Optional[dict[str, Any]]

    task_v1_id: Optional[int]
    task_v2_id: Optional[int]

    # Validators
    @root_validator()
    def task_v1_or_v2(cls, values):
        v1 = values.get("task_v1_id")
        v2 = values.get("task_v2_id")
        if ((v1 is not None) and (v2 is not None)) or (
            (v1 is None) and (v2 is None)
        ):
            message = "both" if (v1 and v2) else "none"
            raise ValueError(
                "One and only one must be provided between "
                f"'task_v1_id' and 'task_v2_id' (you provided {message})"
            )
        return values

    _order = validator("order", allow_reuse=True)(valint("order", min_val=0))
    _filters = validator("filters", allow_reuse=True)(
        val_scalar_dict("filters")
    )


class WorkflowTaskReadV2(BaseModel):

    id: int
    order: Optional[int]
    workflow_id: int
    task_id: int
    is_v2: bool
    task: Union[TaskRead, TaskReadV2]
    meta: Optional[dict[str, Any]]
    args: Optional[dict[str, Any]]
    filters: dict[str, Any]
    # Validators
    _filters = validator("filters", allow_reuse=True)(
        val_scalar_dict("filters")
    )


class WorkflowTaskUpdateV2(BaseModel):

    meta: Optional[dict[str, Any]]
    args: Optional[dict[str, Any]]
    filters: Optional[dict[str, Any]]
    # Validators
    _filters = validator("filters", allow_reuse=True)(
        val_scalar_dict("filters")
    )

    @validator("meta")
    def check_no_parallelisation_level(cls, m):
        if "parallelization_level" in m:
            raise ValueError(
                "Overriding task parallelization level currently not allowed"
            )
        return m


class WorkflowTaskImportV2(BaseModel):

    task: TaskImportV2
    meta: Optional[dict[str, Any]] = None
    args: Optional[dict[str, Any]] = None
    filters: dict[str, Any]
    # Validators
    _filters = validator("filters", allow_reuse=True)(
        val_scalar_dict("filters")
    )


class WorkflowTaskExportV2(BaseModel):

    task: TaskExportV2
    meta: Optional[dict[str, Any]] = None
    args: Optional[dict[str, Any]] = None
    filters: dict[str, Any]
    # Validators
    _filters = validator("filters", allow_reuse=True)(
        val_scalar_dict("filters")
    )
