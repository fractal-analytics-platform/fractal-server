from typing import Any
from typing import Optional

from pydantic import HttpUrl
from sqlalchemy import Column
from sqlalchemy.types import JSON
from sqlmodel import Field
from sqlmodel import SQLModel


class TaskV2(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str

    type: str
    command_non_parallel: Optional[str] = None
    command_parallel: Optional[str] = None
    source: Optional[str] = None

    meta_non_parallel: dict[str, Any] = Field(
        sa_column=Column(JSON, server_default="{}", default={}, nullable=False)
    )
    meta_parallel: dict[str, Any] = Field(
        sa_column=Column(JSON, server_default="{}", default={}, nullable=False)
    )

    version: Optional[str] = None
    args_schema_non_parallel: Optional[dict[str, Any]] = Field(
        sa_column=Column(JSON), default=None
    )
    args_schema_parallel: Optional[dict[str, Any]] = Field(
        sa_column=Column(JSON), default=None
    )
    args_schema_version: Optional[str]
    docs_info: Optional[str] = None
    docs_link: Optional[HttpUrl] = None

    input_types: dict[str, bool] = Field(sa_column=Column(JSON), default={})
    output_types: dict[str, bool] = Field(sa_column=Column(JSON), default={})

    taskgroupv2_id: int = Field(foreign_key="taskgroupv2.id")

    category: Optional[str] = None
    modality: Optional[str] = None
    authors: Optional[str] = None
    tags: list[str] = Field(
        sa_column=Column(JSON, server_default="[]", nullable=False)
    )
