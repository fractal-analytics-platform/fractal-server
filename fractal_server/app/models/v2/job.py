from datetime import datetime
from typing import Any
from typing import Optional

from sqlalchemy import Column
from sqlalchemy.types import DateTime
from sqlalchemy.types import JSON
from sqlmodel import Field
from sqlmodel import SQLModel

from ....utils import get_timestamp
from ...schemas.v2 import JobStatusTypeV2


class JobV2(SQLModel, table=True):
    class Config:
        arbitrary_types_allowed = True

    id: Optional[int] = Field(default=None, primary_key=True)
    project_id: Optional[int] = Field(foreign_key="projectv2.id")
    workflow_id: Optional[int] = Field(foreign_key="workflowv2.id")
    dataset_id: Optional[int] = Field(foreign_key="datasetv2.id")

    user_email: str = Field(nullable=False)
    slurm_account: Optional[str]

    dataset_dump: dict[str, Any] = Field(
        sa_column=Column(JSON, nullable=False)
    )
    workflow_dump: dict[str, Any] = Field(
        sa_column=Column(JSON, nullable=False)
    )
    project_dump: dict[str, Any] = Field(
        sa_column=Column(JSON, nullable=False)
    )

    worker_init: Optional[str]
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
    status: str = JobStatusTypeV2.SUBMITTED
    log: Optional[str] = None
