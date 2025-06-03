from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import Depends
from fastapi import File
from fastapi import Form
from fastapi import HTTPException
from fastapi import Request
from fastapi import Response
from fastapi import status
from fastapi import UploadFile
from packaging.version import InvalidVersion
from packaging.version import parse

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth import current_active_verified_user
from fractal_server.logger import set_logger
from fractal_server.types import NonEmptyStr

# from fractal_server.app.schemas.v2 import TaskGroupActivityV2Read


router = APIRouter()

logger = set_logger(__name__)


@router.post(
    "/collect/pixi/",
    status_code=201,
    # response_model=TaskGroupActivityV2Read,
)
async def collect_task_pixi(
    request: Request,
    response: Response,
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    pixi_version: NonEmptyStr | None = Form(None),
    private: bool = False,
    user_group_id: int | None = None,
    user: UserOAuth = Depends(current_active_verified_user),
    db: AsyncSession = Depends(get_async_db),
):  # -> TaskGroupActivityV2Read:
    filename = file.filename
    if not filename.endswith(".tar.gz"):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"{filename=} does not end with '.tar.gz'.",
        )
    pkg_name = filename.split("-")[0]
    version = filename.removeprefix(f"{pkg_name}-").removesuffix(".tar.gz")
    try:
        parse(version)
    except InvalidVersion:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Detected {pkg_name=} and {version=}, but version is invalid."
            ),
        )
