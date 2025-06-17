from typing import Any

from pydantic import ConfigDict
from sqlalchemy import Column
from sqlalchemy.types import JSON
from sqlmodel import Field
from sqlmodel import Relationship
from sqlmodel import SQLModel

from .task import TaskV2


class WorkflowTaskV2(SQLModel, table=True):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: int | None = Field(default=None, primary_key=True)

    workflow_id: int = Field(foreign_key="workflowv2.id", ondelete="CASCADE")
    order: int | None = None
    meta_parallel: dict[str, Any] | None = Field(
        sa_column=Column(JSON), default=None
    )
    meta_non_parallel: dict[str, Any] | None = Field(
        sa_column=Column(JSON), default=None
    )
    args_parallel: dict[str, Any] | None = Field(
        sa_column=Column(JSON), default=None
    )
    args_non_parallel: dict[str, Any] | None = Field(
        sa_column=Column(JSON), default=None
    )

    type_filters: dict[str, bool] = Field(
        sa_column=Column(JSON, nullable=False, server_default="{}")
    )

    # Task
    task_type: str
    task_id: int = Field(foreign_key="taskv2.id")
    task: TaskV2 = Relationship(sa_relationship_kwargs=dict(lazy="selectin"))
