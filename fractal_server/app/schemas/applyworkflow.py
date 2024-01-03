from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator
from pydantic.types import StrictStr

from ._validators import valstr


__all__ = (
    "_ApplyWorkflowBase",
    "ApplyWorkflowCreate",
    "ApplyWorkflowRead",
)


class ProjectDump(BaseModel):

    model_config = ConfigDict(extra="forbid")

    id: int
    name: str
    read_only: bool
    timestamp_created: datetime


class TaskDump(BaseModel):
    id: int
    source: str
    name: str
    command: str
    input_type: str
    output_type: str
    owner: Optional[str] = None
    version: Optional[str] = None


class WorkflowTaskDump(BaseModel):
    id: int
    order: Optional[int] = None
    workflow_id: int
    task_id: int
    task: TaskDump


class WorkflowDump(BaseModel):
    id: int
    name: str
    project_id: int
    task_list: list[WorkflowTaskDump]


class ResourceDump(BaseModel):
    id: int
    path: str
    dataset_id: int


class DatasetDump(BaseModel):
    id: int
    name: str
    type: Optional[str] = None
    read_only: bool
    resource_list: list[ResourceDump]
    project_id: int


class JobStatusType(str, Enum):
    """
    Define the available job statuses

    Attributes:
        SUBMITTED:
            The workflow has been applied but not yet scheduled with an
            executor. In this phase, due diligence takes place, such as
            creating working directory, assemblying arguments, etc.
        RUNNING:
            The workflow was scheduled with an executor. Note that it might not
            yet be running within the executor, e.g., jobs could still be
            pending within a SLURM executor.
        DONE:
            The workflow was applied successfully
        FAILED:
            The workflow terminated with an error.
    """

    SUBMITTED = "submitted"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"


class _ApplyWorkflowBase(BaseModel):
    """
    Base class for `ApplyWorkflow`.

    Attributes:
        worker_init:
    """

    worker_init: Optional[str] = None


class ApplyWorkflowCreate(_ApplyWorkflowBase):
    """
    Class for `ApplyWorkflow` creation.

    Attributes:
        first_task_index:
        last_task_index:
        slurm_account:
    """

    first_task_index: Optional[int] = Field(
        default=None, validate_default=True
    )
    last_task_index: Optional[int] = Field(default=None, validate_default=True)
    slurm_account: Optional[StrictStr] = None

    # Validators
    _worker_init = field_validator("worker_init")(valstr("worker_init"))

    @field_validator("first_task_index")
    @classmethod
    def first_task_index_non_negative(cls, v):
        """
        Check that `first_task_index` is non-negative.
        """
        if v is not None and v < 0:
            raise ValueError(
                f"first_task_index cannot be negative (given: {v})"
            )
        return v

    @field_validator("last_task_index")
    @classmethod
    def first_last_task_indices(cls, v, info):
        """
        Check that `last_task_index` is non-negative, and that it is not
        smaller than `first_task_index`.
        """
        if v is not None and v < 0:
            raise ValueError(
                f"last_task_index cannot be negative (given: {v})"
            )

        first_task_index = info.data.get("first_task_index")
        last_task_index = v
        if first_task_index is not None and last_task_index is not None:
            if first_task_index > last_task_index:
                raise ValueError(
                    f"{first_task_index=} cannot be larger than "
                    f"{last_task_index=}"
                )
        return v


class ApplyWorkflowRead(_ApplyWorkflowBase):
    """
    Class for `ApplyWorkflow` read from database.

    Attributes:
        id:
        project_id:
        project_dump:
        user_email:
        slurm_account:
        workflow_id:
        workflow_dump:
        input_dataset_id:
        input_dataset_dump:
        output_dataset_id:
        output_dataset_dump:
        start_timestamp:
        end_timestamp:
        status:
        log:
        working_dir:
        working_dir_user:
        first_task_index:
        last_task_index:
    """

    id: int
    project_id: Optional[int] = None
    project_dump: Optional[ProjectDump] = None
    user_email: str
    slurm_account: Optional[str] = None
    workflow_id: Optional[int] = None
    workflow_dump: Optional[WorkflowDump] = None
    input_dataset_id: Optional[int] = None
    input_dataset_dump: Optional[DatasetDump] = None
    output_dataset_id: Optional[int] = None
    output_dataset_dump: Optional[DatasetDump] = None
    start_timestamp: datetime
    end_timestamp: Optional[datetime] = None
    status: str
    log: Optional[str] = None
    working_dir: Optional[str] = None
    working_dir_user: Optional[str] = None
    first_task_index: Optional[int] = None
    last_task_index: Optional[int] = None


class ApplyWorkflowUpdate(BaseModel):
    """
    Class for updating a job status.

    Attributes:
        status: New job status.
    """

    status: JobStatusType
