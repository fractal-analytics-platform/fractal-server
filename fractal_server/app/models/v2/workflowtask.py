from typing import Any
from typing import Optional
from typing import Union

from pydantic import validator
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

    id: Optional[int] = Field(default=None, primary_key=True)

    workflow_id: int = Field(foreign_key="workflowv2.id")
    task_id: int = Field(foreign_key="taskv2.id")
    order: Optional[int]
    meta: Optional[dict[str, Any]] = Field(sa_column=Column(JSON))
    args: Optional[dict[str, Any]] = Field(sa_column=Column(JSON))
    task: TaskV2 = Relationship(sa_relationship_kwargs=dict(lazy="selectin"))

    filters: dict[str, Any] = Field(
        sa_column=Column(JSON, nullable=False, server_default="{}")
    )

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


class WorkflowTaskV2Legacy(SQLModel, table=False):
    pass
