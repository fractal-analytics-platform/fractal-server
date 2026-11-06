from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import UploadFile
from fastapi import status

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.routes.auth import get_api_user
from fractal_server.app.routes.aux.validate_user_profile import (
    validate_user_profile,
)
from fractal_server.app.schemas.v2 import TaskGroupActivityRead
from fractal_server.logger import set_logger

router = APIRouter()

logger = set_logger(__name__)


@router.post(
    "/collect/apptainer/",
    status_code=202,
    response_model=TaskGroupActivityRead,
)
async def collect_task_apptainer(
    response: Response,
    background_tasks: BackgroundTasks,
    file: UploadFile,
    private: bool = False,
    user_group_id: int | None = None,
    user: UserOAuth = Depends(get_api_user),
    db: AsyncSession = Depends(get_async_db),
) -> TaskGroupActivityV2:
    # Get validated resource and profile
    resource, profile = await validate_user_profile(user=user, db=db)
    _ = resource.id

    if not resource.tasks_apptainer_config:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Apptainer task collection is not available.",
        )

    ...
