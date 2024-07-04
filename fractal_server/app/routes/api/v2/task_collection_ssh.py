from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Request
from fastapi import Response
from fastapi import status
from pydantic.error_wrappers import ValidationError

from .....config import get_settings
from .....logger import reset_logger_handlers
from .....logger import set_logger
from .....syringe import Inject
from .....tasks.v2._TaskCollectPip import _TaskCollectPip
from ....db import AsyncSession
from ....db import get_async_db
from ....models.v2 import CollectionStateV2
from ....schemas.v2 import CollectionStateReadV2
from ....schemas.v2 import CollectionStatusV2
from ....schemas.v2 import TaskCollectPipV2
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
    response_model=CollectionStateReadV2,
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
) -> CollectionStateReadV2:
    """
    Task collection endpoint (SSH version)
    """

    # Set default python version
    if task_collect.python_version is None:
        settings = Inject(get_settings)
        task_collect.python_version = (
            settings.FRACTAL_TASKS_PYTHON_DEFAULT_VERSION
        )

    # Validate payload as _TaskCollectPip, which has more strict checks than
    # TaskCollectPip
    try:
        task_pkg = _TaskCollectPip(**task_collect.dict(exclude_unset=True))
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid task-collection object. Original error: {e}",
        )

    # Note: we don't use TaskCollectStatusV2 here for the JSON column `data`
    state = CollectionStateV2(
        data=dict(
            status=CollectionStatusV2.PENDING, package=task_collect.package
        )
    )
    db.add(state)
    await db.commit()

    background_tasks.add_task(
        background_collect_pip_ssh,
        state.id,
        task_pkg,
        request.app.state.connection,
    )

    response.status_code = status.HTTP_201_CREATED
    return state


# FIXME SSH: check_collection_status code is almost identical to the
# one in task_collection.py
@router.get("/collect/{state_id}/", response_model=CollectionStateReadV2)
async def check_collection_status(
    state_id: int,
    verbose: bool = False,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> CollectionStateReadV2:
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

    # FIXME SSH: add logic for when data.state["log"] is empty

    reset_logger_handlers(logger)
    await db.close()
    return state
