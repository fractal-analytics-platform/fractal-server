from datetime import datetime
from typing import Any

from sqlalchemy import ForeignKey
from sqlalchemy import Index
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.types import DateTime
from sqlalchemy.types import String

from fractal_server.app.models.base import Base
from fractal_server.app.schemas.v2 import JobStatusType
from fractal_server.utils import get_timestamp


class JobV2(Base):
    """
    Job table.
    """

    __tablename__ = "jobv2"

    id: Mapped[int] = mapped_column(primary_key=True)
    project_id: Mapped[int | None] = mapped_column(
        ForeignKey("projectv2.id", ondelete="SET NULL"), default=lambda: None
    )
    workflow_id: Mapped[int | None] = mapped_column(
        ForeignKey("workflowv2.id", ondelete="SET NULL"), default=lambda: None
    )
    dataset_id: Mapped[int | None] = mapped_column(
        ForeignKey("datasetv2.id", ondelete="SET NULL"), default=lambda: None
    )

    user_email: Mapped[str] = mapped_column(nullable=False)
    slurm_account: Mapped[str | None] = mapped_column(default=lambda: None)

    dataset_dump: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    workflow_dump: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    project_dump: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    fractal_server_version: Mapped[str] = mapped_column(
        String, server_default="pre-2.19.0", nullable=False
    )

    worker_init: Mapped[str | None] = mapped_column(default=lambda: None)
    working_dir: Mapped[str | None] = mapped_column(default=lambda: None)
    working_dir_user: Mapped[str | None] = mapped_column(default=lambda: None)
    first_task_index: Mapped[int]
    last_task_index: Mapped[int]

    start_timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=get_timestamp
    )
    end_timestamp: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=lambda: None
    )
    status: Mapped[str] = mapped_column(default=JobStatusType.SUBMITTED)
    log: Mapped[str | None] = mapped_column(default=lambda: None)
    executor_error_log: Mapped[str | None] = mapped_column(default=lambda: None)

    attribute_filters: Mapped[dict[str, list[int | float | str | bool]]] = (
        mapped_column(JSONB, nullable=False, server_default="{}")
    )
    type_filters: Mapped[dict[str, bool]] = mapped_column(
        JSONB, nullable=False, server_default="{}"
    )

    __table_args__ = (
        Index(
            "ix_jobv2_one_submitted_job_per_dataset",
            "dataset_id",
            unique=True,
            postgresql_where=text(f"status = '{JobStatusType.SUBMITTED}'"),
        ),
    )
