from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session as DBSyncSession

from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.schemas.v2 import TaskCreateV2


def create_db_tasks_and_update_task_group_sync(
    *,
    task_group_id: int,
    task_list: list[TaskCreateV2],
    db: DBSyncSession,
) -> TaskGroupV2:
    """
    Create a `TaskGroupV2` with N `TaskV2`s, and insert them into the database.

    Arguments:
        task_group_id: ID of an existing `TaskGroupV2` object.
        task_list: List of `TaskCreateV2` objects to be inserted into the db.
        db: Synchronous database session

    Returns:
        Updated `TaskGroupV2` object.
    """
    actual_task_list = [TaskV2(**task.model_dump()) for task in task_list]
    task_group = db.get(TaskGroupV2, task_group_id)
    task_group.task_list = actual_task_list
    db.add(task_group)
    db.commit()
    db.refresh(task_group)

    return task_group


async def create_db_tasks_and_update_task_group_async(
    *,
    task_group_id: int,
    task_list: list[TaskCreateV2],
    db: AsyncSession,
) -> TaskGroupV2:
    """
    Create a `TaskGroupV2` with N `TaskV2`s, and insert them into the database.

    Arguments:
        task_group_id: ID of an existing `TaskGroupV2` object.
        task_list: List of `TaskCreateV2` objects to be inserted into the db.
        db: Synchronous database session

    Returns:
        Updated `TaskGroupV2` object.
    """
    actual_task_list = [TaskV2(**task.model_dump()) for task in task_list]
    task_group = await db.get(TaskGroupV2, task_group_id)
    task_group.task_list = actual_task_list
    db.add(task_group)
    await db.commit()
    await db.refresh(task_group)

    return task_group
