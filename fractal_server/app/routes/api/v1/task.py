from copy import deepcopy  # noqa

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status
from sqlmodel import select

from .....logger import set_logger
from ....db import AsyncSession
from ....db import get_async_db
from ....models.v1 import Task
from ....schemas.v1 import TaskReadV1
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth import current_active_user

router = APIRouter()

logger = set_logger(__name__)


@router.get("/", response_model=list[TaskReadV1])
async def get_list_task(
    user: UserOAuth = Depends(current_active_user),
    args_schema: bool = True,
    db: AsyncSession = Depends(get_async_db),
) -> list[TaskReadV1]:
    """
    Get list of available tasks
    """
    stm = select(Task)
    res = await db.execute(stm)
    task_list = res.scalars().all()
    await db.close()
    if not args_schema:
        for task in task_list:
            setattr(task, "args_schema", None)

    return task_list


@router.get("/{task_id}/", response_model=TaskReadV1)
async def get_task(
    task_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> TaskReadV1:
    """
    Get info on a specific task
    """
    task = await db.get(Task, task_id)
    await db.close()
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Task not found"
        )
    return task
