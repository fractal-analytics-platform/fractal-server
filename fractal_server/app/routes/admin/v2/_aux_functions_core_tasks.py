from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from sqlmodel import select
from sqlmodel import update

from fractal_server.app.db import AsyncSession
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.models.v2 import TaskV2
from fractal_server.types import ListUniqueNonNegativeInt


async def _verify_non_duplication_task_core_constraint(
    *,
    task: TaskV2,
    task_group: TaskGroupV2,
    db: AsyncSession,
) -> None:
    res = await db.execute(
        select(TaskV2.id)
        .where(TaskV2.name == task.name)
        .where(TaskV2.id != task.id)
        .where(TaskV2.is_core.is_(True))
        .join(TaskGroupV2, TaskGroupV2.id == TaskV2.taskgroupv2_id)
        .where(TaskGroupV2.pkg_name == task_group.pkg_name)
        .where(TaskGroupV2.version == task_group.version)
        .where(TaskGroupV2.resource_id == task_group.resource_id)
        .limit(1)
    )
    duplicate_task_id = res.scalar_one_or_none()
    if duplicate_task_id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                "There already exists a core task with "
                f"pkg_name={task_group.pkg_name}, version={task_group.version} "
                f"and name='{task.name}' (task ID: {duplicate_task_id})."
            ),
        )


async def _make_task_core_bulk(
    *,
    task_ids: ListUniqueNonNegativeInt,
    db: AsyncSession,
) -> Response:
    res = await db.execute(
        select(TaskV2, TaskGroupV2)
        .join(TaskGroupV2, TaskGroupV2.id == TaskV2.taskgroupv2_id)
        .where(TaskV2.id.in_(task_ids))
    )
    tasks_and_groups = res.all()
    if len(tasks_and_groups) != len(task_ids):
        missing_ids = sorted(
            list(set(task_ids) - set([tg[0].id for tg in tasks_and_groups]))
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Not all tasks were found (Missing IDs: {missing_ids}).",
        )

    # Acquire a lock on all rows that could result into conflicting core tasks,
    # to avoid a race condition where two "make-core" endpoints are called at
    # the same time. See
    # https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE
    # and https://www.postgresql.org/docs/current/explicit-locking.html#LOCKING-ROWS
    await db.execute(
        select(TaskV2)
        .where(TaskV2.name.in_([t.name for t, _ in tasks_and_groups]))
        .where(TaskV2.version.in_([t.version for t, _ in tasks_and_groups]))
        .where(TaskV2.is_core.is_(False))
        .with_for_update()
    )

    payload_tuples = [
        (
            task.name,
            task_group.pkg_name,
            task_group.version,
            task_group.resource_id,
        )
        for task, task_group in tasks_and_groups
    ]
    if len(set(payload_tuples)) != len(payload_tuples):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                "This request would generate conflicting core tasks "
                "(with the same task name and task-group properties). "
                "Hint: include fewer tasks in the request body and retry."
            ),
        )

    # Non-duplication check constraint
    for task, task_group in tasks_and_groups:
        await _verify_non_duplication_task_core_constraint(
            task=task, task_group=task_group, db=db
        )

    # Update
    await db.execute(
        update(TaskV2).where(TaskV2.id.in_(task_ids)).values(is_core=True)
    )
    await db.commit()

    return Response(
        content=f"{len(task_ids)} tasks have been made core.",
        status_code=status.HTTP_200_OK,
    )


async def _make_task_not_core_bulk(
    *,
    task_ids: ListUniqueNonNegativeInt,
    db: AsyncSession,
) -> Response:
    res = await db.execute(select(TaskV2).where(TaskV2.id.in_(task_ids)))
    tasks = res.scalars().all()
    if len(tasks) != len(task_ids):
        missing_ids = sorted(list(set(task_ids) - set([t.id for t in tasks])))
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Not all tasks were found (Missing IDs: {missing_ids}).",
        )

    # Update
    await db.execute(
        update(TaskV2).where(TaskV2.id.in_(task_ids)).values(is_core=False)
    )
    await db.commit()

    return Response(
        content=f"{len(task_ids)} tasks have been made not core.",
        status_code=status.HTTP_200_OK,
    )
