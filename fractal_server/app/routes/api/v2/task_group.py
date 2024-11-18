from datetime import datetime
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from sqlmodel import or_
from sqlmodel import select

from ._aux_functions_tasks import _get_task_group_full_access
from ._aux_functions_tasks import _get_task_group_read_access
from ._aux_functions_tasks import _verify_non_duplication_group_constraint
from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.app.routes.auth import current_active_user
from fractal_server.app.routes.auth._aux_auth import (
    _verify_user_belongs_to_group,
)
from fractal_server.app.routes.aux import _raise_if_naive_datetime
from fractal_server.app.schemas.v2 import TaskGroupActivityActionV2
from fractal_server.app.schemas.v2 import TaskGroupActivityStatusV2
from fractal_server.app.schemas.v2 import TaskGroupActivityV2Read
from fractal_server.app.schemas.v2 import TaskGroupReadV2
from fractal_server.app.schemas.v2 import TaskGroupUpdateV2
from fractal_server.logger import set_logger

router = APIRouter()

logger = set_logger(__name__)


@router.get("/activity/", response_model=list[TaskGroupActivityV2Read])
async def get_task_group_activity_list(
    task_group_activity_id: Optional[int] = None,
    taskgroupv2_id: Optional[int] = None,
    pkg_name: Optional[str] = None,
    status: Optional[TaskGroupActivityStatusV2] = None,
    action: Optional[TaskGroupActivityActionV2] = None,
    timestamp_started_min: Optional[datetime] = None,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[TaskGroupActivityV2Read]:

    _raise_if_naive_datetime(timestamp_started_min)

    stm = select(TaskGroupActivityV2).where(
        TaskGroupActivityV2.user_id == user.id
    )
    if task_group_activity_id is not None:
        stm = stm.where(TaskGroupActivityV2.id == task_group_activity_id)
    if taskgroupv2_id is not None:
        stm = stm.where(TaskGroupActivityV2.taskgroupv2_id == taskgroupv2_id)
    if pkg_name is not None:
        stm = stm.where(TaskGroupActivityV2.pkg_name.icontains(pkg_name))
    if status is not None:
        stm = stm.where(TaskGroupActivityV2.status == status)
    if action is not None:
        stm = stm.where(TaskGroupActivityV2.action == action)
    if timestamp_started_min is not None:
        stm = stm.where(
            TaskGroupActivityV2.timestamp_started >= timestamp_started_min
        )

    res = await db.execute(stm)
    activities = res.scalars().all()
    return activities


@router.get(
    "/activity/{task_group_activity_id}/",
    response_model=TaskGroupActivityV2Read,
)
async def get_task_group_activity(
    task_group_activity_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> TaskGroupActivityV2Read:

    activity = await db.get(TaskGroupActivityV2, task_group_activity_id)

    if activity is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"TaskGroupActivityV2 {task_group_activity_id} not found",
        )
    if activity.user_id != user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                "You are not the owner of TaskGroupActivityV2 "
                f"{task_group_activity_id}",
            ),
        )

    return activity


@router.get("/", response_model=list[TaskGroupReadV2])
async def get_task_group_list(
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
    only_active: bool = False,
    only_owner: bool = False,
    args_schema: bool = True,
) -> list[TaskGroupReadV2]:
    """
    Get all accessible TaskGroups
    """
    if only_owner:
        condition = TaskGroupV2.user_id == user.id
    else:
        condition = or_(
            TaskGroupV2.user_id == user.id,
            TaskGroupV2.user_group_id.in_(
                select(LinkUserGroup.group_id).where(
                    LinkUserGroup.user_id == user.id
                )
            ),
        )
    stm = select(TaskGroupV2).where(condition)
    if only_active:
        stm = stm.where(TaskGroupV2.active)

    res = await db.execute(stm)
    task_groups = res.scalars().all()

    if args_schema is False:
        for taskgroup in task_groups:
            for task in taskgroup.task_list:
                setattr(task, "args_schema_non_parallel", None)
                setattr(task, "args_schema_parallel", None)

    return task_groups


@router.get("/{task_group_id}/", response_model=TaskGroupReadV2)
async def get_task_group(
    task_group_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> TaskGroupReadV2:
    """
    Get single TaskGroup
    """
    task_group = await _get_task_group_read_access(
        task_group_id=task_group_id,
        user_id=user.id,
        db=db,
    )
    return task_group


@router.delete("/{task_group_id}/", status_code=204)
async def delete_task_group(
    task_group_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
):
    """
    Delete single TaskGroup
    """

    task_group = await _get_task_group_full_access(
        task_group_id=task_group_id,
        user_id=user.id,
        db=db,
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


@router.patch("/{task_group_id}/", response_model=TaskGroupReadV2)
async def patch_task_group(
    task_group_id: int,
    task_group_update: TaskGroupUpdateV2,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> TaskGroupReadV2:
    """
    Patch single TaskGroup
    """
    task_group = await _get_task_group_full_access(
        task_group_id=task_group_id,
        user_id=user.id,
        db=db,
    )
    if (
        "user_group_id" in task_group_update.dict(exclude_unset=True)
        and task_group_update.user_group_id != task_group.user_group_id
    ):
        await _verify_non_duplication_group_constraint(
            db=db,
            pkg_name=task_group.pkg_name,
            version=task_group.version,
            user_group_id=task_group_update.user_group_id,
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
