from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.routes.auth import current_active_user
from fractal_server.app.schemas.v2 import TaskGroupReadV2
from fractal_server.app.schemas.v2 import TaskReadV2
from fractal_server.logger import set_logger

router = APIRouter()

logger = set_logger(__name__)


async def _task_group_access_control(
    *, user: UserOAuth, task_group: TaskGroupV2, db: AsyncSession
):
    if user.id != task_group.user_id:
        if task_group.user_group_id is not None:
            cmd = (
                select(LinkUserGroup)
                .where(LinkUserGroup.user_id == user.id)
                .where(LinkUserGroup.group_id == task_group.user_group_id)
            )
            res = await db.execute(cmd)
            if res.scalars().all() == []:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=(
                        f"TaskGroup {task_group.id} "
                        f"forbidden to user {user.id}"
                    ),
                )
        else:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    f"TaskGroup {task_group.id} forbidden to user {user.id}"
                ),
            )


@router.get("/{task_group_id}", response_model=TaskGroupReadV2)
async def get_task_group(
    task_group_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[TaskReadV2]:
    """
    Get single TaskGroup
    """
    task_group = await db.get(TaskGroupV2, task_group_id)
    await db.close()

    if not task_group:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"TaskGroupV2 {task_group_id} not found.",
        )

    await _task_group_access_control(user=user, task_group=task_group, db=db)
    return task_group
