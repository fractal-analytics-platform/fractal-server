from typing import Any
from typing import Literal

from sqlalchemy import Column
from sqlalchemy.types import JSON
from sqlmodel import Field
from sqlmodel import Relationship
from sqlmodel import SQLModel

from .task import TaskV2


class WorkflowTaskV2(SQLModel, table=True):
    class Config:
        arbitrary_types_allowed = True
        fields = {"parent": {"exclude": True}}

    id: int | None = Field(default=None, primary_key=True)

    workflow_id: int = Field(foreign_key="workflowv2.id")
    order: int | None
    meta_parallel: dict[str, Any] | None = Field(sa_column=Column(JSON))
    meta_non_parallel: dict[str, Any] | None = Field(sa_column=Column(JSON))
    args_parallel: dict[str, Any] | None = Field(sa_column=Column(JSON))
    args_non_parallel: dict[str, Any] | None = Field(sa_column=Column(JSON))

    input_filters: dict[
        Literal["attributes", "types"], dict[str, Any]
    ] = Field(
        sa_column=Column(
            JSON,
            nullable=False,
            server_default='{"attributes": {}, "types": {}}',
        )
    )

    # Task
    task_type: str
    task_id: int = Field(foreign_key="taskv2.id")
    task: TaskV2 = Relationship(sa_relationship_kwargs=dict(lazy="selectin"))
