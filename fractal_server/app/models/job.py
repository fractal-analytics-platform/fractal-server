from datetime import datetime
from enum import Enum
from typing import Any
from typing import Optional

from sqlalchemy import Column
from sqlalchemy import String
from sqlalchemy.types import DateTime
from sqlalchemy.types import JSON
from sqlmodel import Field
from sqlmodel import Relationship
from sqlmodel import SQLModel

from ...utils import get_timestamp
from ..models import Dataset
from ..schemas.applyworkflow import _ApplyWorkflowBase


class JobStatusType(str, Enum):
    """
    Define the job status available

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


class ApplyWorkflow(_ApplyWorkflowBase, SQLModel, table=True):
    """
    Represent a workflow run

    This table is responsible for storing the state of a workflow execution in
    the database.

    Attributes:
        id:
            Primary key.
        project_id:
            ID of the project the workflow belongs to.
        input_dataset_id:
            ID of the input dataset.
        output_dataset_id:
            ID of the output dataset.
        workflow_id:
            ID of the workflow being applied.
        status:
            Workflow status
        workflow_dump:
            Copy of the submitted workflow at the current timestamp.
        start_timestamp:
            Timestamp of when the run began.
        end_timestamp:
            Timestamp of when the run ended or failed.
        status:
            Status of the run.
        log:
            forward of the workflow logs. Usually this attribute is only
            populated upon failure.

        project:
            (Mapper attribute)
        input_dataset:
            (Mapper attribute)
        output_dataset:
            (Mapper attribute)
        workflow:
            (Mapper attribute)
    """

    class Config:
        arbitrary_types_allowed = True

    id: Optional[int] = Field(default=None, primary_key=True)

    project_id: Optional[int] = Field(foreign_key="project.id")

    workflow_id: Optional[int] = Field(foreign_key="workflow.id")
    workflow: Optional["Workflow"] = Relationship(  # noqa: F821
        back_populates="job_list"
    )

    input_dataset_id: Optional[int] = Field(foreign_key="dataset.id")
    input_dataset: Dataset = Relationship(
        sa_relationship_kwargs=dict(
            back_populates="list_jobs_input",
            lazy="selectin",
            primaryjoin="ApplyWorkflow.input_dataset_id==Dataset.id",
        )
    )

    output_dataset_id: Optional[int] = Field(foreign_key="dataset.id")
    output_dataset: Dataset = Relationship(
        sa_relationship_kwargs=dict(
            back_populates="list_jobs_output",
            lazy="selectin",
            primaryjoin="ApplyWorkflow.output_dataset_id==Dataset.id",
        )
    )

    user_dump: str = Field(
        sa_column=Column(String, nullable=False, server_default="")
    )
    input_dataset_dump: dict[str, Any] = Field(
        sa_column=Column(JSON, nullable=False, server_default="{}")
    )
    output_dataset_dump: dict[str, Any] = Field(
        sa_column=Column(JSON, nullable=False, server_default="{}")
    )
    workflow_dump: dict[str, Any] = Field(
        sa_column=Column(JSON, nullable=False, server_default="{}")
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
    status: JobStatusType = JobStatusType.SUBMITTED
    log: Optional[str] = None
