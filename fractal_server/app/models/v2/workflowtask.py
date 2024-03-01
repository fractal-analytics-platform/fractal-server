from typing import Any
from typing import Optional
from typing import Union

from pydantic import validator
from sqlalchemy import Column
from sqlalchemy.types import JSON
from sqlmodel import Field
from sqlmodel import Relationship
from sqlmodel import SQLModel

from ..v1.task import Task
from .task import TaskV2


class WorkflowTaskV2(SQLModel, table=True):
    class Config:
        arbitrary_types_allowed = True
        fields = {"parent": {"exclude": True}}

    id: Optional[int] = Field(default=None, primary_key=True)

    workflow_id: int = Field(foreign_key="workflowv2.id")
    order: Optional[int]
    meta: Optional[dict[str, Any]] = Field(sa_column=Column(JSON))
    args: Optional[dict[str, Any]] = Field(sa_column=Column(JSON))

    filters: dict[str, Any] = Field(
        sa_column=Column(JSON, nullable=False, server_default="{}")
    )

    is_v2: bool = True

    task_v2: Optional[TaskV2] = Relationship(
        sa_relationship_kwargs=dict(lazy="selectin")
    )
    task_v2_id: Optional[int] = Field(foreign_key="taskv2.id")

    task_v1: Optional[Task] = Relationship(
        sa_relationship_kwargs=dict(lazy="selectin")
    )
    task_v1_id: Optional[int] = Field(foreign_key="task.id")

    @property
    def task(self) -> Union[Task, TaskV2]:
        if self.is_v2:
            return self.task_v2
        else:
            return self.task_v1

    @property
    def task_id(self) -> int:
        if self.is_v2:
            return self.task_v2_id
        else:
            return self.task_v1_id

    @property
    def is_parallel(self) -> bool:
        return self.task.is_parallel

    @validator("args")
    def validate_args(cls, value):
        """
        Prevent fractal task reserved parameter names from entering args

        Forbidden argument names are `metadata`,
        `component`.
        """
        if value is None:
            return
        forbidden_args_keys = {
            "metadata",
            "component",
        }
        args_keys = set(value.keys())
        intersect_keys = forbidden_args_keys.intersection(args_keys)
        if intersect_keys:
            raise ValueError(
                "`args` contains the following forbidden keys: "
                f"{intersect_keys}"
            )
        return value
