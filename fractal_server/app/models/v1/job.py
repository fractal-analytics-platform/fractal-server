from datetime import datetime
from typing import Any
from typing import Optional

from sqlalchemy import Column
from sqlalchemy.types import DateTime
from sqlalchemy.types import JSON
from sqlmodel import Field
from sqlmodel import SQLModel

from ....utils import get_timestamp
from ...schemas.v1 import JobStatusTypeV1
from ...schemas.v1.applyworkflow import _ApplyWorkflowBaseV1


class ApplyWorkflow(_ApplyWorkflowBaseV1, SQLModel, table=True):
    """
    Represent a workflow run

    This table is responsible for storing the state of a workflow execution in
    the database.

    Attributes:
        id:
            Primary key.
        project_id:
            ID of the project the workflow belongs to, or `None` if the project
            was deleted.
        input_dataset_id:
            ID of the input dataset, or `None` if the dataset was deleted.
        output_dataset_id:
            ID of the output dataset, or `None` if the dataset was deleted.
        workflow_id:
            ID of the workflow being applied, or `None` if the workflow was
            deleted.
        status:
            Job status
        workflow_dump:
            Copy of the submitted workflow at submission.
        input_dataset_dump:
            Copy of the input_dataset at submission.
        output_dataset_dump:
            Copy of the output_dataset at submission.
        start_timestamp:
            Timestamp of when the run began.
        end_timestamp:
            Timestamp of when the run ended or failed.
        status:
            Status of the run.
        log:
            Forward of the workflow logs.
        user_email:
            Email address of the user who submitted the job.
        slurm_account:
            Account to be used when submitting the job to SLURM (see "account"
            option in [`sbatch`
            documentation](https://slurm.schedmd.com/sbatch.html#SECTION_OPTIONS)).
        first_task_index:
        last_task_index:
    """

    class Config:
        arbitrary_types_allowed = True

    id: Optional[int] = Field(default=None, primary_key=True)

    project_id: Optional[int] = Field(foreign_key="project.id")
    workflow_id: Optional[int] = Field(foreign_key="workflow.id")
    input_dataset_id: Optional[int] = Field(foreign_key="dataset.id")
    output_dataset_id: Optional[int] = Field(foreign_key="dataset.id")

    user_email: str = Field(nullable=False)
    slurm_account: Optional[str]

    input_dataset_dump: dict[str, Any] = Field(
        sa_column=Column(JSON, nullable=False)
    )
    output_dataset_dump: dict[str, Any] = Field(
        sa_column=Column(JSON, nullable=False)
    )
    workflow_dump: dict[str, Any] = Field(
        sa_column=Column(JSON, nullable=False)
    )
    project_dump: dict[str, Any] = Field(
        sa_column=Column(JSON, nullable=False)
    )

    working_dir: Optional[str]
    working_dir_user: Optional[str]
    first_task_index: int
    last_task_index: int

    start_timestamp: datetime = Field(
        default_factory=get_timestamp,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
    end_timestamp: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    status: str = JobStatusTypeV1.SUBMITTED
    log: Optional[str] = None
