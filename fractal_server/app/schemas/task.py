from typing import List

from sqlmodel import SQLModel

from ..schemas import TaskRead


class WorkflowBase(SQLModel):
    name: str
    project_id: int


class WorkflowRead(WorkflowBase):
    id: int
    task_list: List[TaskRead]
