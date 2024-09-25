from datetime import datetime
from enum import Enum
from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import validator

from .._validators import valint
from .._validators import valstr
from .._validators import valutc
from .project import ProjectReadV1
from .task import TaskExportV1
from .task import TaskReadV1

__all__ = (
    "WorkflowReadV1",
    "WorkflowExportV1",
    "WorkflowTaskExportV1",
    "WorkflowTaskReadV1",
    "WorkflowTaskStatusTypeV1",
)


class _WorkflowTaskBaseV1(BaseModel):
    """
    Base class for `WorkflowTask`.
    """

    meta: Optional[dict[str, Any]] = None
    args: Optional[dict[str, Any]] = None


class WorkflowTaskReadV1(_WorkflowTaskBaseV1):
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
    task: TaskReadV1


class WorkflowTaskExportV1(_WorkflowTaskBaseV1):
    """
    Class for `WorkflowTask` export.

    Attributes:
        task:
    """

    task: TaskExportV1


class _WorkflowBaseV1(BaseModel):
    """
    Base class for `Workflow`.

    Attributes:
        name: Workflow name.
    """

    name: str


class WorkflowReadV1(_WorkflowBaseV1):
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
    task_list: list[WorkflowTaskReadV1]
    project: ProjectReadV1
    timestamp_created: datetime

    _timestamp_created = validator("timestamp_created", allow_reuse=True)(
        valutc("timestamp_created")
    )


class WorkflowExportV1(_WorkflowBaseV1):
    """
    Class for `Workflow` export.

    Attributes:
        task_list:
    """

    task_list: list[WorkflowTaskExportV1]


class WorkflowTaskStatusTypeV1(str, Enum):
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
