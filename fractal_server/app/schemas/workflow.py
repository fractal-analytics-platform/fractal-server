from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from sqlmodel import SQLModel

from .task import TaskRead

__all__ = ("WorkflowRead", "WorkflowCreate", "WorkflowTaskCreate")


class _WorkflowTaskBase(SQLModel):
    workflow_id: Optional[int]
    task_id: Optional[int]
    order: Optional[int]
    args: Dict[str, Any]


class WorkflowTaskCreate(_WorkflowTaskBase):
    pass


class _WorkflowBase(SQLModel):
    name: str
    project_id: int


class WorkflowRead(_WorkflowBase):
    id: int
    task_list: List[TaskRead]


class WorkflowCreate(_WorkflowBase):
    pass
