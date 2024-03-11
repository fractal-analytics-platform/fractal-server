from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status

from .....logger import set_logger
from ....db import AsyncSession
from ....db import get_async_db
from ....schemas import StateRead
from ....schemas.v2 import TaskCollectPipV2
from ....security import current_active_user
from ....security import current_active_verified_user
from ....security import User

router = APIRouter()

logger = set_logger(__name__)


@router.post(
    "/collect/pip/",
    response_model=StateRead,
    responses={
        201: dict(
            description=(
                "Task collection successfully started in the background"
            )
        ),
        200: dict(
            description=(
                "Package already collected. Returning info on already "
                "available tasks"
            )
        ),
    },
)
async def collect_tasks_pip(
    task_collect: TaskCollectPipV2,
    background_tasks: BackgroundTasks,
    response: Response,
    user: User = Depends(current_active_verified_user),
    db: AsyncSession = Depends(get_async_db),
) -> StateRead:  # State[TaskCollectStatus]
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Not implemented yet.",
    )


@router.get("/collect/{state_id}/", response_model=StateRead)
async def check_collection_status(
    state_id: int,
    user: User = Depends(current_active_user),
    verbose: bool = False,
    db: AsyncSession = Depends(get_async_db),
) -> StateRead:  # State[TaskCollectStatus]
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Not implemented yet.",
    )
