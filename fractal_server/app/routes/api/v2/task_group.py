from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from sqlmodel import or_
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.app.routes.auth import current_active_user
from fractal_server.app.schemas.v2 import TaskGroupReadV2
from fractal_server.app.schemas.v2 import TaskGroupUpdateStrictV2
from fractal_server.logger import set_logger

router = APIRouter()

logger = set_logger(__name__)


@router.get("/", response_model=list[TaskGroupReadV2])
async def get_task_group_list(
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[TaskGroupReadV2]:
    """
    Get all accessible TaskGroups
    """
    stm = select(TaskGroupV2).where(
        or_(
            TaskGroupV2.user_id == user.id,
            TaskGroupV2.user_group_id.in_(
                select(LinkUserGroup.group_id).where(
                    LinkUserGroup.user_id == user.id
                )
            ),
        )
    )
    res = await db.execute(stm)
    task_groups = res.scalars().all()

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
    task_group = await db.get(TaskGroupV2, task_group_id)
    if task_group is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"TaskGroupV2 {task_group_id} not found.",
        )

    if task_group.user_id != user.id:
        stm = (
            select(LinkUserGroup)
            .where(LinkUserGroup.user_id == user.id)
            .where(LinkUserGroup.group_id == task_group.user_group_id)
        )
        res = await db.execute(stm)
        if res.scalars().all() == []:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    f"TaskGroup {task_group.id} forbidden to user {user.id}"
                ),
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
    task_group = await db.get(TaskGroupV2, task_group_id)
    if task_group is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"TaskGroupV2 {task_group_id} not found.",
        )

    if task_group.user_id != user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"TaskGroup {task_group.id} forbidden to user {user.id}",
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

    await db.delete(task_group)
    await db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch("/{task_group_id}/", response_model=TaskGroupReadV2)
async def patch_task_group(
    task_group_id: int,
    task_group_update: TaskGroupUpdateStrictV2,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> TaskGroupReadV2:
    """
    Patch single TaskGroup
    """
    task_group = await db.get(TaskGroupV2, task_group_id)
    if task_group is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"TaskGroupV2 {task_group_id} not found.",
        )

    if task_group.user_id != user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"TaskGroup {task_group.id} forbidden to user {user.id}",
        )

    for key, value in task_group_update.dict(exclude_unset=True).items():
        setattr(task_group, key, value)

    db.add(task_group)
    await db.commit()
    await db.refresh(task_group)
    await db.close()
    return task_group
