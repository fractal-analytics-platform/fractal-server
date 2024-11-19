from datetime import datetime
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from sqlalchemy.sql.operators import is_
from sqlalchemy.sql.operators import is_not
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.app.routes.auth import current_active_superuser
from fractal_server.app.routes.auth._aux_auth import (
    _verify_user_belongs_to_group,
)
from fractal_server.app.routes.aux import _raise_if_naive_datetime
from fractal_server.app.schemas.v2 import TaskGroupActivityActionV2
from fractal_server.app.schemas.v2 import TaskGroupActivityStatusV2
from fractal_server.app.schemas.v2 import TaskGroupActivityV2Read
from fractal_server.app.schemas.v2 import TaskGroupReadV2
from fractal_server.app.schemas.v2 import TaskGroupUpdateV2
from fractal_server.app.schemas.v2 import TaskGroupV2OriginEnum
from fractal_server.logger import set_logger

router = APIRouter()

logger = set_logger(__name__)


@router.get("/activity/", response_model=list[TaskGroupActivityV2Read])
async def get_task_group_activity_list(
    task_group_activity_id: Optional[int] = None,
    user_id: Optional[int] = None,
    taskgroupv2_id: Optional[int] = None,
    pkg_name: Optional[str] = None,
    status: Optional[TaskGroupActivityStatusV2] = None,
    action: Optional[TaskGroupActivityActionV2] = None,
    timestamp_started_min: Optional[datetime] = None,
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> list[TaskGroupActivityV2Read]:

    _raise_if_naive_datetime(timestamp_started_min)

    stm = select(TaskGroupActivityV2)
    if task_group_activity_id is not None:
        stm = stm.where(TaskGroupActivityV2.id == task_group_activity_id)
    if user_id:
        stm = stm.where(TaskGroupActivityV2.user_id == user_id)
    if taskgroupv2_id:
        stm = stm.where(TaskGroupActivityV2.taskgroupv2_id == taskgroupv2_id)
    if pkg_name:
        stm = stm.where(TaskGroupActivityV2.pkg_name.icontains(pkg_name))
    if status:
        stm = stm.where(TaskGroupActivityV2.status == status)
    if action:
        stm = stm.where(TaskGroupActivityV2.action == action)
    if timestamp_started_min is not None:
        stm = stm.where(
            TaskGroupActivityV2.timestamp_started >= timestamp_started_min
        )

    res = await db.execute(stm)
    activities = res.scalars().all()
    return activities


@router.get("/{task_group_id}/", response_model=TaskGroupReadV2)
async def query_task_group(
    task_group_id: int,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> TaskGroupReadV2:

    task_group = await db.get(TaskGroupV2, task_group_id)
    if task_group is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"TaskGroup {task_group_id} not found",
        )
    return task_group


@router.get("/", response_model=list[TaskGroupReadV2])
async def query_task_group_list(
    user_id: Optional[int] = None,
    user_group_id: Optional[int] = None,
    private: Optional[bool] = None,
    active: Optional[bool] = None,
    pkg_name: Optional[str] = None,
    origin: Optional[TaskGroupV2OriginEnum] = None,
    timestamp_last_used_min: Optional[datetime] = None,
    timestamp_last_used_max: Optional[datetime] = None,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> list[TaskGroupReadV2]:

    stm = select(TaskGroupV2)

    _raise_if_naive_datetime(
        timestamp_last_used_max,
        timestamp_last_used_min,
    )

    if user_group_id is not None and private is True:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "Cannot get task groups with both "
                f"{user_group_id=} and {private=}."
            ),
        )
    if user_id is not None:
        stm = stm.where(TaskGroupV2.user_id == user_id)
    if user_group_id is not None:
        stm = stm.where(TaskGroupV2.user_group_id == user_group_id)
    if private is not None:
        if private is True:
            stm = stm.where(is_(TaskGroupV2.user_group_id, None))
        else:
            stm = stm.where(is_not(TaskGroupV2.user_group_id, None))
    if active is not None:
        if active is True:
            stm = stm.where(is_(TaskGroupV2.active, True))
        else:
            stm = stm.where(is_(TaskGroupV2.active, False))
    if origin is not None:
        stm = stm.where(TaskGroupV2.origin == origin)
    if pkg_name is not None:
        stm = stm.where(TaskGroupV2.pkg_name.icontains(pkg_name))
    if timestamp_last_used_min is not None:
        stm = stm.where(
            TaskGroupV2.timestamp_last_used >= timestamp_last_used_min
        )
    if timestamp_last_used_max is not None:
        stm = stm.where(
            TaskGroupV2.timestamp_last_used <= timestamp_last_used_max
        )

    res = await db.execute(stm)
    task_groups_list = res.scalars().all()
    return task_groups_list


@router.patch("/{task_group_id}/", response_model=TaskGroupReadV2)
async def patch_task_group(
    task_group_id: int,
    task_group_update: TaskGroupUpdateV2,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> list[TaskGroupReadV2]:
    task_group = await db.get(TaskGroupV2, task_group_id)
    if task_group is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"TaskGroupV2 {task_group_id} not found",
        )

    for key, value in task_group_update.dict(exclude_unset=True).items():
        if (key == "user_group_id") and (value is not None):
            await _verify_user_belongs_to_group(
                user_id=user.id, user_group_id=value, db=db
            )
        setattr(task_group, key, value)

    db.add(task_group)
    await db.commit()
    await db.refresh(task_group)
    return task_group


@router.delete("/{task_group_id}/", status_code=204)
async def delete_task_group(
    task_group_id: int,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
):
    task_group = await db.get(TaskGroupV2, task_group_id)
    if task_group is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"TaskGroupV2 {task_group_id} not found",
        )

    stm = select(WorkflowTaskV2).where(
        WorkflowTaskV2.task_id.in_({task.id for task in task_group.task_list})
    )
    res = await db.execute(stm)
    workflow_tasks = res.scalars().all()
    if workflow_tasks != []:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"TaskV2 {workflow_tasks[0].task_id} is still in use",
        )

    # Cascade operations: set foreign-keys to null for TaskGroupActivityV2
    # which are in relationship with the current TaskGroupV2
    logger.debug("Start of cascade operations on TaskGroupActivityV2.")
    stm = select(TaskGroupActivityV2).where(
        TaskGroupActivityV2.taskgroupv2_id == task_group_id
    )
    res = await db.execute(stm)
    task_group_activity_list = res.scalars().all()
    for task_group_activity in task_group_activity_list:
        logger.debug(
            f"Setting TaskGroupActivityV2[{task_group_activity.id}]"
            ".taskgroupv2_id to None."
        )
        task_group_activity.taskgroupv2_id = None
        db.add(task_group_activity)
    logger.debug("End of cascade operations on TaskGroupActivityV2.")

    await db.delete(task_group)
    await db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)
