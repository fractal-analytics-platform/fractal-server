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
from .task import TaskImportV1
from .task import TaskReadV1

__all__ = (
    "WorkflowCreateV1",
    "WorkflowReadV1",
    "WorkflowUpdateV1",
    "WorkflowImportV1",
    "WorkflowExportV1",
    "WorkflowTaskCreateV1",
    "WorkflowTaskImportV1",
    "WorkflowTaskExportV1",
    "WorkflowTaskReadV1",
    "WorkflowTaskUpdateV1",
    "WorkflowTaskStatusTypeV1",
)


class _WorkflowTaskBaseV1(BaseModel):
    """
    Base class for `WorkflowTask`.
    """

    meta: Optional[dict[str, Any]] = None
    args: Optional[dict[str, Any]] = None


class WorkflowTaskCreateV1(_WorkflowTaskBaseV1):
    """
    Class for `WorkflowTask` creation.

    Attributes:
        order:
    """

    order: Optional[int]
    # Validators
    _order = validator("order", allow_reuse=True)(valint("order", min_val=0))


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


class WorkflowTaskImportV1(_WorkflowTaskBaseV1):
    """
    Class for `WorkflowTask` import.

    Attributes:
        task:
    """

    task: TaskImportV1


class WorkflowTaskExportV1(_WorkflowTaskBaseV1):
    """
    Class for `WorkflowTask` export.

    Attributes:
        task:
    """

    task: TaskExportV1


class WorkflowTaskUpdateV1(_WorkflowTaskBaseV1):
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


class WorkflowCreateV1(_WorkflowBaseV1):
    """
    Task for `Workflow` creation.
    """

    # Validators
    _name = validator("name", allow_reuse=True)(valstr("name"))


class WorkflowUpdateV1(_WorkflowBaseV1):
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


class WorkflowImportV1(_WorkflowBaseV1):
    """
    Class for `Workflow` import.

    Attributes:
        task_list:
    """

    task_list: list[WorkflowTaskImportV1]

    # Validators
    _name = validator("name", allow_reuse=True)(valstr("name"))


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
