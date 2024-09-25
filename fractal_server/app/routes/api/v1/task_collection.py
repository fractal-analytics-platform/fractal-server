from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status

from .....logger import close_logger
from .....logger import set_logger
from ....db import AsyncSession
from ....db import get_async_db
from ....models.v1 import State
from ....schemas.v1 import StateRead
from ....schemas.v1 import TaskCollectStatusV1
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth import current_active_user
from fractal_server.tasks.utils import get_collection_log

router = APIRouter()

logger = set_logger(__name__)


@router.get("/collect/{state_id}/", response_model=StateRead)
async def check_collection_status(
    state_id: int,
    user: UserOAuth = Depends(current_active_user),
    verbose: bool = False,
    db: AsyncSession = Depends(get_async_db),
) -> StateRead:  # State[TaskCollectStatus]
    """
    Check status of background task collection
    """
    logger = set_logger(logger_name="check_collection_status")
    logger.debug(f"Querying state for state.id={state_id}")
    state = await db.get(State, state_id)
    if not state:
        await db.close()
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No task collection info with id={state_id}",
        )
    data = TaskCollectStatusV1(**state.data)

    # In some cases (i.e. a successful or ongoing task collection), data.log is
    # not set; if so, we collect the current logs
    if verbose and not data.log:
        data.log = get_collection_log(data.venv_path)
        state.data = data.sanitised_dict()
    close_logger(logger)
    await db.close()
    return state
