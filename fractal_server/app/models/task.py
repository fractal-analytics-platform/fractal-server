from typing import Any
from typing import Optional

from sqlalchemy import Column
from sqlalchemy.types import JSON
from sqlmodel import Field

from ...common.schemas.task import _TaskBase


class Task(_TaskBase, table=True):
    """
    Task model

    Attributes:
        id: Primary key
        command: TBD
        input_type: TBD
        output_type: TBD
        default_args: TBD
        meta: TBD
        source: inherited from `_TaskBase`
        name: inherited from `_TaskBase`
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    command: str
    input_type: str
    output_type: str
    default_args: Optional[dict[str, Any]] = Field(
        sa_column=Column(JSON), default={}
    )
    meta: Optional[dict[str, Any]] = Field(sa_column=Column(JSON), default={})

    @property
    def parallelization_level(self) -> Optional[str]:
        try:
            return self.meta["parallelization_level"]
        except KeyError:
            return None

    @property
    def is_parallel(self) -> bool:
        return bool(self.parallelization_level)
