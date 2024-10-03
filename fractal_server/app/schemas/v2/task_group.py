from typing import Optional

from pydantic import BaseModel

from .task import TaskReadV2


class TaskGroupCreateV2(BaseModel):
    active: bool = True


class TaskGroupReadV2(BaseModel):

    id: int
    user_id: int
    user_group_id: Optional[int] = None
    active: bool
    task_list: list[TaskReadV2]


class TaskGroupUpdateV2(BaseModel):
    user_group_id: Optional[int] = None
    active: Optional[bool] = None
