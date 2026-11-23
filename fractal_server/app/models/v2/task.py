from typing import Any

from sqlalchemy import BOOLEAN
from sqlalchemy import ForeignKey
from sqlalchemy import Index
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column

from fractal_server.app.models.base import Base


class TaskV2(Base):
    """
    Model for the `taskv2` database table.

    Attributes:
        id:
        name:
        type:
        version:
        command_non_parallel:
        command_parallel:
        meta_non_parallel:
        meta_parallel:
        input_types:
        output_types:
        taskgroupv2_id:
        args_schema_version:
        args_schema_non_parallel:
        args_schema_parallel:
        docs_info:
        docs_link:
        category:
        modality:
        authors:
        tags:
    """

    __tablename__ = "taskv2"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]

    type: Mapped[str]
    command_non_parallel: Mapped[str | None] = mapped_column(
        default=lambda: None
    )
    command_parallel: Mapped[str | None] = mapped_column(default=lambda: None)

    meta_non_parallel: Mapped[dict[str, Any]] = mapped_column(
        JSON, server_default="{}", default={}, nullable=False
    )
    meta_parallel: Mapped[dict[str, Any]] = mapped_column(
        JSON, server_default="{}", default={}, nullable=False
    )

    version: Mapped[str]
    args_schema_non_parallel: Mapped[dict[str, Any] | None] = mapped_column(
        JSON, default=lambda: None
    )
    args_schema_parallel: Mapped[dict[str, Any] | None] = mapped_column(
        JSON, default=lambda: None
    )
    args_schema_version: Mapped[str | None] = mapped_column(
        default=lambda: None
    )
    docs_info: Mapped[str | None] = mapped_column(default=lambda: None)
    docs_link: Mapped[str | None] = mapped_column(default=lambda: None)

    input_types: Mapped[dict[str, bool]] = mapped_column(
        JSONB, nullable=True, default={}
    )
    output_types: Mapped[dict[str, bool]] = mapped_column(
        JSONB, nullable=True, default={}
    )

    taskgroupv2_id: Mapped[int] = mapped_column(ForeignKey("taskgroupv2.id"))

    category: Mapped[str | None] = mapped_column(default=lambda: None)
    modality: Mapped[str | None] = mapped_column(default=lambda: None)
    authors: Mapped[str | None] = mapped_column(default=lambda: None)
    tags: Mapped[list[str]] = mapped_column(
        JSONB, server_default="[]", nullable=False
    )
    is_core: Mapped[bool] = mapped_column(
        BOOLEAN,
        server_default="false",
        nullable=False,
    )
    __table_args__ = (
        Index(
            "ix_taskv2_one_task_name_per_task_group",
            "name",
            "taskgroupv2_id",
            unique=True,
        ),
    )
