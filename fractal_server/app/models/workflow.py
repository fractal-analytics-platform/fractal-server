from typing import Any
from typing import Optional
from typing import Union

from pydantic import validator
from sqlalchemy import Column
from sqlalchemy.ext.orderinglist import ordering_list
from sqlalchemy.types import JSON
from sqlmodel import Field
from sqlmodel import Relationship

from ...common.schemas.workflow import _WorkflowBase
from ...common.schemas.workflow import _WorkflowTaskBase
from ..db import AsyncSession
from .task import Task


class WorkflowTask(_WorkflowTaskBase, table=True):
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
            Additional task arguments, overriding the ones in
            `WorkflowTask.task.args`
        task:
            `Task` object associated with the current `WorkflowTask`

    """

    class Config:
        arbitrary_types_allowed = True
        fields = {"parent": {"exclude": True}}

    id: Optional[int] = Field(default=None, primary_key=True)

    workflow_id: Optional[int] = Field(foreign_key="workflow.id")
    task_id: Optional[int] = Field(foreign_key="task.id")
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
    def arguments(self):
        """
        Override default arguments
        """
        out = self.task.default_args.copy()
        out.update(self.args or {})
        return out

    @property
    def is_parallel(self) -> bool:
        return self.task.is_parallel

    @property
    def parallelization_level(self) -> Union[str, None]:
        return self.task.parallelization_level

    @property
    def overridden_meta(self) -> dict:
        """
        Return a combination of self.meta (higher priority) and self.task.meta
        (lower priority) key-value pairs.
        """
        res = self.task.meta.copy() or {}
        res.update(self.meta or {})
        return res

    def assemble_args(self, extra: dict[str, Any] = None) -> dict:
        """
        Merge of `extra` arguments and `self.arguments`.

        Returns
            full_args:
                A dictionary consisting of the merge of `extra` and
                `self.arguments`.
        """
        full_args = {}
        if extra:
            full_args.update(extra)
        full_args.update(self.arguments)
        return full_args


class Workflow(_WorkflowBase, table=True):
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

    task_list: list["WorkflowTask"] = Relationship(
        sa_relationship_kwargs=dict(
            lazy="selectin",
            order_by="WorkflowTask.order",
            collection_class=ordering_list("order"),
            cascade="all, delete-orphan",
        ),
    )

    async def insert_task(
        self,
        task_id: int,
        *,
        args: Optional[dict[str, Any]] = None,
        meta: Optional[dict[str, Any]] = None,
        order: Optional[int] = None,
        db: AsyncSession,
        commit: bool = True,
    ) -> WorkflowTask:
        """
        Insert a new WorkflowTask into Workflow.task_list

        Args:
            task_id: TBD
            args: TBD
            meta: TBD
            order: TBD
            db: TBD
            commit: TBD
        """
        if order is None:
            order = len(self.task_list)
        wf_task = WorkflowTask(task_id=task_id, args=args, meta=meta)
        db.add(wf_task)
        self.task_list.insert(order, wf_task)
        self.task_list.reorder()  # type: ignore
        if commit:
            await db.commit()
            await db.refresh(wf_task)
        return wf_task

    @property
    def input_type(self):
        return self.task_list[0].task.input_type
