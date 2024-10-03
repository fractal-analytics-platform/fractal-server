from typing import Optional

from sqlalchemy.orm import Session as DBSyncSession

from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.schemas.v2 import TaskCreateV2
from fractal_server.app.schemas.v2 import TaskGroupCreateV2


def _get_task_type(task: TaskCreateV2) -> str:
    if task.command_non_parallel is None:
        return "parallel"
    elif task.command_parallel is None:
        return "non_parallel"
    else:
        return "compound"


def create_db_task_group_and_tasks(
    *,
    task_list: list[TaskCreateV2],
    task_group_obj: TaskGroupCreateV2,
    user_id: int,
    db: DBSyncSession,
    user_group_id: Optional[int] = None,
) -> TaskGroupV2:
    """
    Create a `TaskGroupV2` with N `TaskV2`s, and insert them into the database.

    Arguments:
        task_group:
        task_list:
        user_id:
        user_group_id: Can be `None`
        db: A synchronous database session
    """
    actual_task_list = [
        TaskV2(
            **task.dict(),
            type=_get_task_type(task),
        )
        for task in task_list
    ]
    task_group = TaskGroupV2(
        user_id=user_id,
        user_group_id=user_group_id,
        task_list=actual_task_list,
        **task_group_obj.dict(),
    )
    db.add(task_group)
    db.commit()
    db.refresh(task_group)
    return task_group
