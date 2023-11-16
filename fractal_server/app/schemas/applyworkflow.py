from datetime import datetime
from enum import Enum
from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import validator

from ._validators import valstr

__all__ = (
    "_ApplyWorkflowBase",
    "ApplyWorkflowCreate",
    "ApplyWorkflowRead",
)


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


class _ApplyWorkflowWithValidStatus(BaseModel):
    """
    Model to be used for a validator, within `ApplyWorkflow` model
    """

    status: JobStatusType = JobStatusType.SUBMITTED


class _ApplyWorkflowBase(BaseModel):
    """
    Base class for `ApplyWorkflow`.

    Attributes:
        worker_init:
    """

    worker_init: Optional[str]


class ApplyWorkflowCreate(_ApplyWorkflowBase):
    """
    Class for `ApplyWorkflow` creation.

    Attributes:
        first_task_index:
        last_task_index:
    """

    first_task_index: Optional[int] = None
    last_task_index: Optional[int] = None

    # Validators
    _worker_init = validator("worker_init", allow_reuse=True)(
        valstr("worker_init")
    )

    @validator("first_task_index", always=True)
    def first_task_index_non_negative(cls, v, values):
        """
        Check that `first_task_index` is non-negative.
        """
        if v is not None and v < 0:
            raise ValueError(
                f"first_task_index cannot be negative (given: {v})"
            )
        return v

    @validator("last_task_index", always=True)
    def first_last_task_indices(cls, v, values):
        """
        Check that `last_task_index` is non-negative, and that it is not
        smaller than `first_task_index`.
        """
        if v is not None and v < 0:
            raise ValueError(
                f"last_task_index cannot be negative (given: {v})"
            )

        first_task_index = values.get("first_task_index")
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
        workflow_id:
        input_dataset_id:
        output_dataset_id:
        start_timestamp:
        end_timestamp:
        status:
        log:
        workflow_dump:
        history:
        working_dir:
        working_dir_user:
        first_task_index:
        last_task_index:
    """

    id: int
    project_id: int
    user_email: str
    workflow_id: int
    workflow_dump: Optional[dict[str, Any]]
    input_dataset_id: int
    input_dataset_dump: Optional[dict[str, Any]]
    output_dataset_id: int
    output_dataset_dump: Optional[dict[str, Any]]
    start_timestamp: datetime
    end_timestamp: Optional[datetime]
    status: str
    log: Optional[str]
    working_dir: Optional[str]
    working_dir_user: Optional[str]
    first_task_index: Optional[int]
    last_task_index: Optional[int]

    def sanitised_dict(self):
        d = self.dict()
        d["start_timestamp"] = str(self.start_timestamp)
        d["end_timestamp"] = str(self.end_timestamp)
        return d
