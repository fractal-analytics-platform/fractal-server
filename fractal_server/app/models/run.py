from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional

from sqlalchemy import Column
from sqlalchemy.types import DateTime
from sqlmodel import Field
from sqlmodel import Relationship

from ...common.schemas import ApplyWorkflowBase
from ...config import get_settings
from ...syringe import Inject
from ...utils import get_timestamp
from .project import Dataset
from .project import Project
from .workflow import Workflow


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


class ApplyWorkflow(ApplyWorkflowBase, table=True):
    """
    Represent a workflow being apply

    This table is responsible for storing the state of a workflow execution in
    the DB.

    Attributes:
        ...:
            Workflow submission parameters
        status:
            Workflow status
        log:
            forward of the workflow logs. Usually this attribute is only
            populated upon failure.

    """

    id: Optional[int] = Field(default=None, primary_key=True)
    project_id: int = Field(foreign_key="project.id")
    input_dataset_id: int = Field(foreign_key="dataset.id")
    output_dataset_id: int = Field(foreign_key="dataset.id")
    workflow_id: int = Field(foreign_key="workflow.id")

    project: Project = Relationship()
    input_dataset: Dataset = Relationship(
        sa_relationship_kwargs=dict(
            lazy="selectin",
            primaryjoin="ApplyWorkflow.input_dataset_id==Dataset.id",
        )
    )
    output_dataset: Dataset = Relationship(
        sa_relationship_kwargs=dict(
            lazy="selectin",
            primaryjoin="ApplyWorkflow.output_dataset_id==Dataset.id",
        )
    )
    workflow: Workflow = Relationship()

    start_timestamp: datetime = Field(
        default_factory=get_timestamp,
        nullable=False,
        sa_column=Column(DateTime(timezone=True)),
    )
    status: JobStatusType = JobStatusType.SUBMITTED
    log: Optional[str] = None

    @property
    def job_root_path(self) -> Path:
        settings = Inject(get_settings)
        return settings.FRACTAL_RUNNER_WORKING_BASE_DIR / f"job_{self.id:06d}"

    @property
    def log_path(self) -> Path:
        return self.job_root_path / "job.log"

    def make_job_dir(self):
        _path = self.job_root_path
        if not _path.is_dir():
            _path.mkdir(exists_ok=True, parents=True)
