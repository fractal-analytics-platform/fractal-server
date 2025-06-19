from datetime import datetime
from typing import Any

from pydantic import ConfigDict
from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.types import DateTime
from sqlmodel import Field
from sqlmodel import SQLModel

from ....utils import get_timestamp
from ...schemas.v2 import JobStatusTypeV2
from fractal_server.types import AttributeFilters


class JobV2(SQLModel, table=True):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: int | None = Field(default=None, primary_key=True)
    project_id: int | None = Field(
        foreign_key="projectv2.id", default=None, ondelete="SET NULL"
    )
    workflow_id: int | None = Field(
        foreign_key="workflowv2.id", default=None, ondelete="SET NULL"
    )
    dataset_id: int | None = Field(
        foreign_key="datasetv2.id", default=None, ondelete="SET NULL"
    )

    user_email: str = Field(nullable=False)
    slurm_account: str | None = None

    dataset_dump: dict[str, Any] = Field(
        sa_column=Column(JSONB, nullable=False)
    )
    workflow_dump: dict[str, Any] = Field(
        sa_column=Column(JSONB, nullable=False)
    )
    project_dump: dict[str, Any] = Field(
        sa_column=Column(JSONB, nullable=False)
    )

    worker_init: str | None = None
    working_dir: str | None = None
    working_dir_user: str | None = None
    first_task_index: int
    last_task_index: int

    start_timestamp: datetime = Field(
        default_factory=get_timestamp,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
    end_timestamp: datetime | None = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    status: str = JobStatusTypeV2.SUBMITTED
    log: str | None = None

    attribute_filters: AttributeFilters = Field(
        sa_column=Column(JSONB, nullable=False, server_default="{}")
    )
    type_filters: dict[str, bool] = Field(
        sa_column=Column(JSONB, nullable=False, server_default="{}")
    )
