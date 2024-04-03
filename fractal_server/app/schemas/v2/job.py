from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel
from pydantic import Extra
from pydantic import validator
from pydantic.types import StrictStr

from .._validators import valstr
from .._validators import valutc
from .dumps import DatasetDumpV2
from .dumps import ProjectDumpV2
from .dumps import WorkflowDumpV2


class JobStatusTypeV2(str, Enum):
    """
    Define the available job statuses

    Attributes:
        SUBMITTED:
            The job was created. This does not guarantee that it was also
            submitted to an executor (e.g. other errors could have prevented
            this), nor that it is actually running (e.g. SLURM jobs could be
            still in the queue).
        DONE:
            The job successfully reached its end.
        FAILED:
            The workflow terminated with an error.
    """

    SUBMITTED = "submitted"
    DONE = "done"
    FAILED = "failed"


class JobCreateV2(BaseModel, extra=Extra.forbid):

    first_task_index: Optional[int] = None
    last_task_index: Optional[int] = None
    slurm_account: Optional[StrictStr] = None
    worker_init: Optional[str]

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


class JobReadV2(BaseModel):

    id: int
    project_id: Optional[int]
    project_dump: ProjectDumpV2
    user_email: str
    slurm_account: Optional[str]
    workflow_id: Optional[int]
    workflow_dump: WorkflowDumpV2
    dataset_id: Optional[int]
    dataset_dump: DatasetDumpV2
    start_timestamp: datetime
    end_timestamp: Optional[datetime]
    status: str
    log: Optional[str]
    working_dir: Optional[str]
    working_dir_user: Optional[str]
    first_task_index: Optional[int]
    last_task_index: Optional[int]
    worker_init: Optional[str]

    _start_timestamp = validator("start_timestamp", allow_reuse=True)(
        valutc("start_timestamp")
    )
    _end_timestamp = validator("end_timestamp", allow_reuse=True)(
        valutc("end_timestamp")
    )


class JobUpdateV2(BaseModel):

    status: JobStatusTypeV2
