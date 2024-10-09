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
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.app.routes.auth import current_active_superuser
from fractal_server.app.routes.auth._aux_auth import (
    _verify_user_belongs_to_group,
)
from fractal_server.app.schemas.v2 import TaskGroupReadV2
from fractal_server.app.schemas.v2 import TaskGroupUpdateV2

router = APIRouter()


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
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> list[TaskGroupReadV2]:

    stm = select(TaskGroupV2)

    if user_group_id is not None and private is True:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Cannot set `user_group_id` with {private=}",
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

    await db.delete(task_group)
    await db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)