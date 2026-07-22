from typing import Any

from sqlalchemy import ForeignKey
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship

from fractal_server.app.models.base import Base

from .task import TaskV2


class WorkflowTaskV2(Base):
    __tablename__ = "workflowtaskv2"

    id: Mapped[int] = mapped_column(primary_key=True)

    workflow_id: Mapped[int] = mapped_column(
        ForeignKey("workflowv2.id", ondelete="CASCADE")
    )
    order: Mapped[int | None] = mapped_column(default=lambda: None)
    meta_parallel: Mapped[dict[str, Any] | None] = mapped_column(
        JSON, default=lambda: None
    )
    meta_non_parallel: Mapped[dict[str, Any] | None] = mapped_column(
        JSON, default=lambda: None
    )
    args_parallel: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB, default=lambda: None
    )
    args_non_parallel: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB, default=lambda: None
    )

    type_filters: Mapped[dict[str, bool]] = mapped_column(
        JSONB, nullable=False, server_default="{}"
    )

    # Task
    task_type: Mapped[str] = mapped_column()
    task_id: Mapped[int] = mapped_column(ForeignKey("taskv2.id"))
    task: Mapped["TaskV2"] = relationship(lazy="selectin")

    alias: Mapped[str | None] = mapped_column(
        default=lambda: None, nullable=True
    )
    description: Mapped[str | None] = mapped_column(
        default=lambda: None, nullable=True
    )
