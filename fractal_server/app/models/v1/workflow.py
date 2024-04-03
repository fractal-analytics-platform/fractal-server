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
from ...schemas.v1.workflow import _WorkflowBaseV1
from ...schemas.v1.workflow import _WorkflowTaskBaseV1
from .task import Task


class WorkflowTask(_WorkflowTaskBaseV1, SQLModel, table=True):
    """
    A Task as part of a Workflow

    This is a crossing table between Task and Workflow. In addition to the
    foreign keys, it allows for parameter overriding and keeps the order
    within the list of tasks of the workflow.


    Attributes:
        id:
            Primary key
        workflow_id:
            ID of the `Workflow` the `WorkflowTask` belongs to
        task_id:
            ID of the task corresponding to the `WorkflowTask`
        order:
            Positional order of the `WorkflowTask` in `Workflow.task_list`
        meta:
            Additional parameters useful for execution
        args:
            Task arguments
        task:
            `Task` object associated with the current `WorkflowTask`

    """

    class Config:
        arbitrary_types_allowed = True
        fields = {"parent": {"exclude": True}}

    id: Optional[int] = Field(default=None, primary_key=True)

    workflow_id: int = Field(foreign_key="workflow.id")
    task_id: int = Field(foreign_key="task.id")
    order: Optional[int]
    meta: Optional[dict[str, Any]] = Field(sa_column=Column(JSON))
    args: Optional[dict[str, Any]] = Field(sa_column=Column(JSON))
    task: Task = Relationship(sa_relationship_kwargs=dict(lazy="selectin"))

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


class Workflow(_WorkflowBaseV1, SQLModel, table=True):
    """
    Workflow

    Attributes:
        id:
            Primary key
        project_id:
            ID of the project the workflow belongs to.
        task_list:
            List of associations to tasks.
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    project_id: int = Field(foreign_key="project.id")
    project: "Project" = Relationship(  # noqa: F821
        sa_relationship_kwargs=dict(lazy="selectin"),
    )

    task_list: list[WorkflowTask] = Relationship(
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
