from datetime import datetime
from enum import Enum
from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import validator

from ._validators import valint
from ._validators import valstr
from ._validators import valutc
from .project import ProjectRead
from .task import TaskExport
from .task import TaskImport
from .task import TaskRead

__all__ = (
    "WorkflowCreate",
    "WorkflowRead",
    "WorkflowUpdate",
    "WorkflowImport",
    "WorkflowExport",
    "WorkflowTaskCreate",
    "WorkflowTaskImport",
    "WorkflowTaskExport",
    "WorkflowTaskRead",
    "WorkflowTaskUpdate",
    "WorkflowTaskStatusType",
)


class _WorkflowTaskBase(BaseModel):
    """
    Base class for `WorkflowTask`.
    """

    meta: Optional[dict[str, Any]] = None
    args: Optional[dict[str, Any]] = None


class WorkflowTaskCreate(_WorkflowTaskBase):
    """
    Class for `WorkflowTask` creation.

    Attributes:
        order:
    """

    order: Optional[int]
    # Validators
    _order = validator("order", allow_reuse=True)(valint("order", min_val=0))


class WorkflowTaskRead(_WorkflowTaskBase):
    """
    Class for `WorkflowTask` read from database.

    Attributes:
        id:
        order:
        workflow_id:
        task_id:
        task:
    """

    id: int
    order: Optional[int]
    workflow_id: int
    task_id: int
    task: TaskRead


class WorkflowTaskImport(_WorkflowTaskBase):
    """
    Class for `WorkflowTask` import.

    Attributes:
        task:
    """

    task: TaskImport


class WorkflowTaskExport(_WorkflowTaskBase):
    """
    Class for `WorkflowTask` export.

    Attributes:
        task:
    """

    task: TaskExport


class WorkflowTaskUpdate(_WorkflowTaskBase):
    """
    Class for `WorkflowTask` update.
    """

    # Validators
    @validator("meta")
    def check_no_parallelisation_level(cls, m):
        if "parallelization_level" in m:
            raise ValueError(
                "Overriding task parallelization level currently not allowed"
            )
        return m


class _WorkflowBase(BaseModel):
    """
    Base class for `Workflow`.

    Attributes:
        name: Workflow name.
    """

    name: str


class WorkflowRead(_WorkflowBase):
    """
    Task for `Workflow` read from database.

    Attributes:
        id:
        project_id:
        task_list:
        project:
    """

    id: int
    project_id: int
    task_list: list[WorkflowTaskRead]
    project: ProjectRead
    timestamp_created: datetime

    _timestamp_created = validator("timestamp_created", allow_reuse=True)(
        valutc("timestamp_created")
    )


class WorkflowCreate(_WorkflowBase):
    """
    Task for `Workflow` creation.
    """

    # Validators
    _name = validator("name", allow_reuse=True)(valstr("name"))


class WorkflowUpdate(_WorkflowBase):
    """
    Task for `Workflow` update.

    Attributes:
        name:
        reordered_workflowtask_ids:
    """

    name: Optional[str]
    reordered_workflowtask_ids: Optional[list[int]]

    # Validators
    _name = validator("name", allow_reuse=True)(valstr("name"))

    @validator("reordered_workflowtask_ids")
    def check_positive_and_unique(cls, value):
        if any(i < 0 for i in value):
            raise ValueError("Negative `id` in `reordered_workflowtask_ids`")
        if len(value) != len(set(value)):
            raise ValueError("`reordered_workflowtask_ids` has repetitions")
        return value


class WorkflowImport(_WorkflowBase):
    """
    Class for `Workflow` import.

    Attributes:
        task_list:
    """

    task_list: list[WorkflowTaskImport]

    # Validators
    _name = validator("name", allow_reuse=True)(valstr("name"))


class WorkflowExport(_WorkflowBase):
    """
    Class for `Workflow` export.

    Attributes:
        task_list:
    """

    task_list: list[WorkflowTaskExport]


class WorkflowTaskStatusType(str, Enum):
    """
    Define the available values for the status of a `WorkflowTask`.

    This model is used within the `Dataset.history` attribute, which is
    constructed in the runner and then used in the API (e.g. in the
    `api/v1/project/{project_id}/dataset/{dataset_id}/status` endpoint).

    Attributes:
        SUBMITTED: The `WorkflowTask` is part of a running job.
        DONE: The most-recent execution of this `WorkflowTask` was successful.
        FAILED: The most-recent execution of this `WorkflowTask` failed.
    """

    SUBMITTED = "submitted"
    DONE = "done"
    FAILED = "failed"
