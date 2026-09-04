from datetime import datetime
from typing import Any

from sqlalchemy import ForeignKey
from sqlalchemy import Index
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.types import DateTime

from fractal_server.app.models.base import Base
from fractal_server.utils import get_timestamp


class WorkflowTemplate(Base):
    """
    Model for the `workflowtemplate` database table.

    Attributes:
        id:
        user_id:
        name:
        version:
        fractal_server_version:
        timestamp_created:
        timestamp_last_used:
        user_group_id:
        description:
        data:
    """

    __tablename__ = "workflowtemplate"

    id: Mapped[int] = mapped_column(primary_key=True)

    user_id: Mapped[int] = mapped_column(
        ForeignKey("user_oauth.id"), nullable=False
    )
    name: Mapped[str]
    version: Mapped[int]

    fractal_server_version: Mapped[str]
    timestamp_created: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=get_timestamp
    )
    timestamp_last_used: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=get_timestamp
    )

    user_group_id: Mapped[int | None] = mapped_column(
        ForeignKey("usergroup.id", ondelete="SET NULL"), default=lambda: None
    )

    description: Mapped[str | None] = mapped_column(default=lambda: None)
    data: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)

    __table_args__ = (
        Index(
            "ix_workflowtemplate_user_name_version_unique_constraint",
            "user_id",
            "name",
            "version",
            unique=True,
        ),
    )
