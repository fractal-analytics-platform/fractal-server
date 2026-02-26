from datetime import datetime
from typing import Any

from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.types import DateTime
from sqlmodel import Field
from sqlmodel import Index
from sqlmodel import SQLModel

from fractal_server.utils import get_timestamp


class WorkflowTemplate(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)

    user_id: int = Field(foreign_key="user_oauth.id", nullable=False)
    name: str
    version: int

    timestamp_created: datetime = Field(
        default_factory=get_timestamp,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
    timestamp_last_used: datetime | None = Field(
        default=None,
        sa_column=Column(DateTime(timezone=True)),
    )

    user_group_id: int | None = Field(
        foreign_key="usergroup.id", default=None, ondelete="SET NULL"
    )

    description: str | None = None
    data: dict[str, Any] = Field(sa_column=Column(JSON, nullable=False))

    __table_args__ = (
        Index(
            "ix_workflowtemplate_user_name_version_unique_constraint",
            "user_id",
            "name",
            "version",
            unique=True,
        ),
    )
