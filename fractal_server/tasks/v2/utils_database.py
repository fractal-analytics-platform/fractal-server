from sqlalchemy.orm import Session as DBSyncSession

from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.schemas.v2 import TaskCreateV2


def _get_task_type(task: TaskCreateV2) -> str:
    if task.command_non_parallel is None:
        return "parallel"
    elif task.command_parallel is None:
        return "non_parallel"
    else:
        return "compound"


def create_db_tasks_and_update_task_group(
    *,
    task_group_id: int,
    task_list: list[TaskCreateV2],
    db: DBSyncSession,
) -> TaskGroupV2:
    """
    Create a `TaskGroupV2` with N `TaskV2`s, and insert them into the database.

    Arguments:
        task_group: ID of an existing TaskGroupV2 object.
        task_list: A list of TaskCreateV2 objects to be inserted into the db.
        db: A synchronous database session
    """
    actual_task_list = [
        TaskV2(
            **task.dict(),
            type=_get_task_type(task),
        )
        for task in task_list
    ]
    task_group = db.get(TaskGroupV2, task_group_id)
    task_group.task_list = actual_task_list
    db.add(task_group)
    db.commit()
    db.refresh(task_group)

    return task_group
