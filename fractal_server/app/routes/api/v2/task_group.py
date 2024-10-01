from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status
from sqlmodel import or_
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import UserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.routes.auth import current_active_user
from fractal_server.app.schemas.v2 import TaskGroupReadV2
from fractal_server.app.schemas.v2 import TaskGroupUpdateV2
from fractal_server.logger import set_logger

router = APIRouter()

logger = set_logger(__name__)


def _access_control(user: UserOAuth):
    return or_(
        TaskGroupV2.user_id == user.id,
        TaskGroupV2.user_group_id.in_(
            select(LinkUserGroup.group_id).where(
                LinkUserGroup.user_id == user.id
            )
        ),
    )


@router.get("/", response_model=list[TaskGroupReadV2])
async def get_task_group_list(
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[TaskGroupReadV2]:
    """
    Get all accessible TaskGroups
    """

    cmd = select(TaskGroupV2).where(_access_control(user))
    res = await db.execute(cmd)
    task_groups = res.scalars().all()

    return task_groups


@router.get("/{task_group_id}", response_model=TaskGroupReadV2)
async def get_task_group(
    task_group_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> TaskGroupReadV2:
    """
    Get single TaskGroup
    """
    cmd = (
        select(TaskGroupV2)
        .where(TaskGroupV2.id == task_group_id)
        .where(_access_control(user))
    )
    res = await db.execute(cmd)
    task_group = res.scalars().one_or_none()

    if task_group is None:
        task_group = await db.get(TaskGroupV2, task_group_id)
        if task_group is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=(
                    f"TaskGroupV2 {task_group_id} do not exists ornot found."
                ),
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    f"TaskGroup {task_group.id} forbidden to user {user.id}"
                ),
            )

    return task_group


@router.patch("/{task_group_id}", response_model=TaskGroupReadV2)
async def patch_task_group(
    task_group_id: int,
    task_group_update: TaskGroupUpdateV2,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[TaskGroupReadV2]:
    """
    Patch single TaskGroups
    """

    cmd = (
        select(TaskGroupV2)
        .where(TaskGroupV2.id == task_group_id)
        .where(_access_control(user))
    )
    res = await db.execute(cmd)
    task_group = res.scalars().one_or_none()

    if task_group is None:
        task_group = await db.get(TaskGroupV2, task_group_id)
        if task_group is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"TaskGroupV2 {task_group_id} does not exist.",
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    f"TaskGroup {task_group.id} forbidden to user {user.id}"
                ),
            )

    if task_group_update.user_group_id is not None:
        user_group = await db.get(UserGroup, task_group_update.user_group_id)
        if user_group is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=(
                    f"UserGroup {task_group_update.user_group_id} "
                    "does not exist."
                ),
            )
        cmd = (
            select(LinkUserGroup)
            .where(LinkUserGroup.group_id == task_group_update.user_group_id)
            .where(LinkUserGroup.user_id == current_active_user.id)
        )
        res = await db.execute(cmd)
        if res.scalars().all() == []:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    f"User {current_active_user.id} does not belong to "
                    f"UserGroup {task_group_update.user_group_id}."
                ),
            )
    else:
        if task_group_update.dict(exclude_unset=True) == {}:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Nothing to update.",
            )

    task_group.user_group_id = task_group_update.user_group_id
    db.add(task_group)
    await db.commit()
    await db.refresh(task_group)
    return task_group
