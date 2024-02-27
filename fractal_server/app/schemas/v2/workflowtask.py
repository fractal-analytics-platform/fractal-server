from enum import Enum
from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import validator

from .._validators import valint
from ..task import TaskExport  # FIXME V2
from ..task import TaskImport  # FIXME V2
from ..task import TaskRead  # FIXME V2
from fractal_server.app.schemas.v2._validators_v2 import val_scalar_dict


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
    # Validators
    _order = validator("order", allow_reuse=True)(valint("order", min_val=0))
    _filters = validator("filters", allow_reuse=True)(
        val_scalar_dict("filters")
    )


class WorkflowTaskReadV2(BaseModel):

    id: int
    order: Optional[int]
    workflow_id: int
    task_id: int
    task: TaskRead
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

    task: TaskImport
    meta: Optional[dict[str, Any]] = None
    args: Optional[dict[str, Any]] = None
    filters: dict[str, Any]
    # Validators
    _filters = validator("filters", allow_reuse=True)(
        val_scalar_dict("filters")
    )


class WorkflowTaskExportV2(BaseModel):

    task: TaskExport
    meta: Optional[dict[str, Any]] = None
    args: Optional[dict[str, Any]] = None
    filters: dict[str, Any]
    # Validators
    _filters = validator("filters", allow_reuse=True)(
        val_scalar_dict("filters")
    )
