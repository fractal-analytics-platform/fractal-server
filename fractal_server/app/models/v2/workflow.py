from datetime import datetime
from typing import Any
from typing import Optional
from typing import Union

from pydantic import validator
from sqlalchemy import Column
from sqlalchemy.ext.orderinglist import ordering_list
from sqlalchemy.types import DateTime
from sqlalchemy.types import JSON
from sqlmodel import Field
from sqlmodel import Relationship
from sqlmodel import SQLModel

from ....utils import get_timestamp
from .task import TaskV2


class WorkflowTaskV2(SQLModel, table=True):
    class Config:
        arbitrary_types_allowed = True
        fields = {"parent": {"exclude": True}}

    id: Optional[int] = Field(default=None, primary_key=True)

    workflow_id: int = Field(foreign_key="workflow.id")
    task_id: int = Field(foreign_key="task.id")
    order: Optional[int]
    meta: Optional[dict[str, Any]] = Field(sa_column=Column(JSON))
    args: Optional[dict[str, Any]] = Field(sa_column=Column(JSON))
    task: TaskV2 = Relationship(sa_relationship_kwargs=dict(lazy="selectin"))

    filters: dict[str, Any] = Field(sa_column=Column(JSON))

    @validator("args")
    def validate_args(cls, value: dict = None):
        """
        Prevent fractal task reserved parameter names from entering args

        Forbidden argument names are `input_paths`, `output_path`, `metadata`,
        `component`.
        """
        if value is None:
            return
        forbidden_args_keys = {
            "input_paths",
            "output_path",
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

    @property
    def is_parallel(self) -> bool:
        return self.task.is_parallel

    @property
    def parallelization_level(self) -> Union[str, None]:
        return self.task.parallelization_level


class WorkflowV2(SQLModel, table=True):

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    project_id: int = Field(foreign_key="project.id")
    project: "Project" = Relationship(  # noqa: F821
        sa_relationship_kwargs=dict(lazy="selectin"),
    )

    task_list: list[WorkflowTaskV2] = Relationship(
        sa_relationship_kwargs=dict(
            lazy="selectin",
            order_by="WorkflowTask.order",
            collection_class=ordering_list("order"),
            cascade="all, delete-orphan",
        ),
    )
    timestamp_created: datetime = Field(
        default_factory=get_timestamp,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )

    @property
    def input_type(self):
        return self.task_list[0].task.input_type

    @property
    def output_type(self):
        return self.task_list[-1].task.output_type


class WorkflowTaskV2Legacy(SQLModel, table=False):
    pass
