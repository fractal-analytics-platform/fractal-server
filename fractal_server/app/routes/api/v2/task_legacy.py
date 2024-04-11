from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status
from sqlmodel import select

from .....logger import set_logger
from ....db import AsyncSession
from ....db import get_async_db
from ....models.v1 import Task as TaskV1
from ....schemas.v2 import TaskLegacyReadV2
from ....security import current_active_user
from ....security import User

router = APIRouter()

logger = set_logger(__name__)


@router.get("/", response_model=list[TaskLegacyReadV2])
async def get_list_task_legacy(
    args_schema: bool = True,
    only_v2_compatible: bool = False,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[TaskLegacyReadV2]:
    """
    Get list of available legacy tasks
    """
    stm = select(TaskV1)
    if only_v2_compatible:
        stm = stm.where(TaskV1.is_v2_compatible)
    res = await db.execute(stm)
    task_list = res.scalars().all()
    await db.close()
    if args_schema is False:
        for task in task_list:
            setattr(task, "args_schema", None)

    return task_list


@router.get("/{task_id}/", response_model=TaskLegacyReadV2)
async def get_task_legacy(
    task_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> TaskLegacyReadV2:
    """
    Get info on a specific legacy task
    """
    task = await db.get(TaskV1, task_id)
    await db.close()
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"TaskV1[{task_id}] not found",
        )
    return task
