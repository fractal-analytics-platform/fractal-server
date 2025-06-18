from typing import Any

from sqlalchemy import Column
from sqlalchemy.types import JSON
from sqlmodel import Field
from sqlmodel import SQLModel


class TaskV2(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str

    type: str
    command_non_parallel: str | None = None
    command_parallel: str | None = None
    source: str | None = None

    meta_non_parallel: dict[str, Any] = Field(
        sa_column=Column(JSON, server_default="{}", default={}, nullable=False)
    )
    meta_parallel: dict[str, Any] = Field(
        sa_column=Column(JSON, server_default="{}", default={}, nullable=False)
    )

    version: str | None = None
    args_schema_non_parallel: dict[str, Any] | None = Field(
        sa_column=Column(JSON), default=None
    )
    args_schema_parallel: dict[str, Any] | None = Field(
        sa_column=Column(JSON), default=None
    )
    args_schema_version: str | None = None
    docs_info: str | None = None
    docs_link: str | None = None

    input_types: dict[str, bool] = Field(sa_column=Column(JSON), default={})
    output_types: dict[str, bool] = Field(sa_column=Column(JSON), default={})

    taskgroupv2_id: int = Field(foreign_key="taskgroupv2.id")

    category: str | None = None
    modality: str | None = None
    authors: str | None = None
    tags: list[str] = Field(
        sa_column=Column(JSON, server_default="[]", nullable=False)
    )
