from datetime import datetime
from typing import Any

from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.types import DateTime

from fractal_server.app.models.base import Base
from fractal_server.utils import get_timestamp


class HistoryRun(Base):
    """
    HistoryRun table.
    """

    __tablename__ = "historyrun"

    id: Mapped[int] = mapped_column(primary_key=True)
    dataset_id: Mapped[int] = mapped_column(
        ForeignKey("datasetv2.id", ondelete="CASCADE"),
    )
    workflowtask_id: Mapped[int | None] = mapped_column(
        ForeignKey("workflowtaskv2.id", ondelete="SET NULL"),
        default=lambda: None,
    )
    job_id: Mapped[int] = mapped_column(ForeignKey("jobv2.id"))
    task_id: Mapped[int | None] = mapped_column(
        ForeignKey("taskv2.id", ondelete="SET NULL")
    )

    workflowtask_dump: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False
    )
    task_group_dump: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False
    )

    timestamp_started: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=get_timestamp
    )
    status: Mapped[str] = mapped_column()
    num_available_images: Mapped[int] = mapped_column()


class HistoryUnit(Base):
    """
    HistoryUnit table.
    """

    __tablename__ = "historyunit"

    id: Mapped[int] = mapped_column(primary_key=True)
    history_run_id: Mapped[int] = mapped_column(
        ForeignKey("historyrun.id", ondelete="CASCADE"),
        index=True,
    )

    logfile: Mapped[str] = mapped_column()
    has_warnings: Mapped[bool] = mapped_column(default=False)
    status: Mapped[str] = mapped_column()
    zarr_urls: Mapped[list[str]] = mapped_column(
        ARRAY(String),
        nullable=True,
        default=list,
    )


class HistoryImageCache(Base):
    """
    HistoryImageCache table.
    """

    __tablename__ = "historyimagecache"

    zarr_url: Mapped[str] = mapped_column(primary_key=True)
    dataset_id: Mapped[int] = mapped_column(
        ForeignKey("datasetv2.id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )
    workflowtask_id: Mapped[int] = mapped_column(
        ForeignKey("workflowtaskv2.id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )

    latest_history_unit_id: Mapped[int] = mapped_column(
        ForeignKey("historyunit.id", ondelete="CASCADE"),
        index=True,
    )
