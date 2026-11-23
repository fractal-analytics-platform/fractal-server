from fastapi import HTTPException
from fastapi import status
from sqlalchemy import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.models.v2 import TaskV2


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
                f"pkg_name='{task_group.pkg_name}', "
                f"version='{task_group.version}' and name='{task.name}' "
                f"(task ID: {duplicate_task_id})."
            ),
        )
