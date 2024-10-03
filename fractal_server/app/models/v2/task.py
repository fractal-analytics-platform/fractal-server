from datetime import datetime
from typing import Any
from typing import Optional

from pydantic import HttpUrl
from sqlalchemy import Column
from sqlalchemy.types import DateTime
from sqlalchemy.types import JSON
from sqlmodel import Field
from sqlmodel import Relationship
from sqlmodel import SQLModel

from fractal_server.utils import get_timestamp


class TaskV2(SQLModel, table=True):

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str

    type: str
    command_non_parallel: Optional[str] = None
    command_parallel: Optional[str] = None
    source: str = Field(unique=True)

    meta_non_parallel: dict[str, Any] = Field(
        sa_column=Column(JSON, server_default="{}", default={}, nullable=False)
    )
    meta_parallel: dict[str, Any] = Field(
        sa_column=Column(JSON, server_default="{}", default={}, nullable=False)
    )

    owner: Optional[str] = None
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

    taskgroupv2_id: Optional[int] = Field(foreign_key="taskgroupv2.id")


class TaskGroupV2(SQLModel, table=True):

    id: Optional[int] = Field(default=None, primary_key=True)

    user_id: int = Field(foreign_key="user_oauth.id")
    user_group_id: Optional[int] = Field(foreign_key="usergroup.id")

    active: bool = True
    task_list: list[TaskV2] = Relationship(
        sa_relationship_kwargs=dict(
            lazy="selectin", cascade="all, delete-orphan"
        ),
    )

    timestamp_created: datetime = Field(
        default_factory=get_timestamp,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
