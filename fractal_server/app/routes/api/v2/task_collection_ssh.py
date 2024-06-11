from pathlib import Path

from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Request
from fastapi import Response
from fastapi import status

from .....logger import reset_logger_handlers
from .....logger import set_logger
from ....db import AsyncSession
from ....db import get_async_db
from ....models.v2 import CollectionStateV2
from ....schemas.state import StateRead
from ....schemas.v2 import TaskCollectPipV2
from ....schemas.v2 import TaskCollectStatusV2
from ....security import current_active_user
from ....security import current_active_verified_user
from ....security import User
from fractal_server.tasks.v2.background_operations_ssh import (
    background_collect_pip_ssh,
)

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
    request: Request,
    user: User = Depends(current_active_verified_user),
    db: AsyncSession = Depends(get_async_db),
) -> StateRead:
    """
    Task collection endpoint

    Trigger the creation of a dedicated virtual environment, the installation
    of a package and the collection of tasks as advertised in the manifest.
    """

    # Create State object (after casting venv_path to string)
    collection_status = TaskCollectStatusV2(
        status="pending",
        package=task_collect.package,
        venv_path=Path("/dummy"),  # FIXME: this is spurious
    )
    collection_status_dict = collection_status.dict()
    collection_status_dict["venv_path"] = str(collection_status.venv_path)
    state = CollectionStateV2(data=collection_status.sanitised_dict())
    db.add(state)
    await db.commit()

    background_tasks.add_task(
        background_collect_pip_ssh,
        task_collect,
        state.id,
        request.app.state.connection,
    )

    response.status_code = status.HTTP_201_CREATED
    return state


@router.get("/collect/{state_id}/", response_model=StateRead)
async def check_collection_status(
    state_id: int,
    verbose: bool = False,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> StateRead:  # State[TaskCollectStatus]
    """
    Check status of background task collection
    """
    logger = set_logger(logger_name="check_collection_status")
    logger.debug(f"Querying state for state.id={state_id}")
    state = await db.get(CollectionStateV2, state_id)
    if not state:
        await db.close()
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No task collection info with id={state_id}",
        )
    data = TaskCollectStatusV2(**state.data)

    # In some cases (i.e. a successful or ongoing task collection), data.log is
    # not set; if so, we collect the current logs
    if verbose and not data.log:
        state.data = data.sanitised_dict()
    reset_logger_handlers(logger)
    await db.close()
    return state
