from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel
from pydantic import Extra
from pydantic import Field
from pydantic import field_validator
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

    first_task_index: Optional[int] = Field(
        default=None, validate_default=True
    )
    last_task_index: Optional[int] = Field(default=None, validate_default=True)
    slurm_account: Optional[StrictStr] = None
    worker_init: Optional[str] = None

    # Validators
    _worker_init = field_validator("worker_init")(valstr("worker_init"))

    @field_validator("first_task_index")
    def first_task_index_non_negative(cls, v):
        """
        Check that `first_task_index` is non-negative.
        """
        if v is not None and v < 0:
            raise ValueError(
                f"first_task_index cannot be negative (given: {v})"
            )
        return v

    # !
    @field_validator("last_task_index")
    def first_last_task_indices(cls, v, values):
        """
        Check that `last_task_index` is non-negative, and that it is not
        smaller than `first_task_index`.
        """
        if v is not None and v < 0:
            raise ValueError(
                f"last_task_index cannot be negative (given: {v})"
            )

        first_task_index = values.data.get("first_task_index")
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
    project_id: Optional[int] = None
    project_dump: ProjectDumpV2
    user_email: str
    slurm_account: Optional[str] = None
    workflow_id: Optional[int] = None
    workflow_dump: WorkflowDumpV2
    dataset_id: Optional[int] = None
    dataset_dump: DatasetDumpV2
    start_timestamp: datetime
    end_timestamp: Optional[datetime] = None
    status: str
    log: Optional[str] = None
    working_dir: Optional[str] = None
    working_dir_user: Optional[str] = None
    first_task_index: Optional[int] = None
    last_task_index: Optional[int] = None
    worker_init: Optional[str] = None

    _start_timestamp = field_validator("start_timestamp")(
        valutc("start_timestamp")
    )
    _end_timestamp = field_validator("end_timestamp")(valutc("end_timestamp"))


class JobUpdateV2(BaseModel):

    status: JobStatusTypeV2
