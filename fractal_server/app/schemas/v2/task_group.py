from typing import Optional

from pydantic import BaseModel

from .task import TaskReadV2


class TaskGroupReadV2(BaseModel):

    id: int
    user_id: int
    user_group_id: Optional[int] = None
    active: bool
    task_list: list[TaskReadV2]


class TaskGroupUpdateStrictV2(BaseModel):

    active: bool
