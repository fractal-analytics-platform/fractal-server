from typing import Any
from typing import Optional

from sqlalchemy import Column
from sqlalchemy import UniqueConstraint
from sqlalchemy.types import JSON
from sqlmodel import Field
from sqlmodel import SQLModel

from ...common.schemas.task import _TaskBase


class Task(_TaskBase, SQLModel, table=True):
    """
    Task model

    Attributes:
        id: Primary key
        command: Executable command
        input_type: Expected type of input `Dataset`
        output_type: Expected type of output `Dataset`
        meta:
            Additional metadata related to execution (e.g. computational
            resources)
        source: inherited from `_TaskBase`
        name: inherited from `_TaskBase`
        args_schema: JSON schema of task arguments
        args_schema_version:
            label pointing at how the JSON schema of task arguments was
            generated
    """

    __table_args__ = (
        UniqueConstraint("input_type", name="unique_task_input_type"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    command: str
    source: str = Field(unique=True)
    input_type: str
    output_type: str
    meta: Optional[dict[str, Any]] = Field(sa_column=Column(JSON), default={})
    owner: Optional[str] = None
    version: Optional[str] = None
    args_schema: Optional[dict[str, Any]] = Field(
        sa_column=Column(JSON), default=None
    )
    args_schema_version: Optional[str]

    @property
    def parallelization_level(self) -> Optional[str]:
        try:
            return self.meta["parallelization_level"]
        except KeyError:
            return None

    @property
    def is_parallel(self) -> bool:
        return bool(self.parallelization_level)
