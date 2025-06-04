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

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth import current_active_verified_user
from fractal_server.config import get_settings
from fractal_server.logger import set_logger
from fractal_server.syringe import Inject
from fractal_server.types import NonEmptyStr

# from fractal_server.app.schemas.v2 import TaskGroupActivityV2Read


router = APIRouter()

logger = set_logger(__name__)


def get_pkgname_and_version(filename: str) -> tuple[str, str]:
    if not filename.endswith(".tar.gz"):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"{filename=} does not end with '.tar.gz'.",
        )
    filename_splitted = filename.split("-")
    if len(filename_splitted) != 2:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Invalid filename format: '{filename}' contains "
                f"{len(filename_splitted) - 1} hyphen(s), but exactly one is "
                "required to separate the package name from the version "
                "(expected format: 'pkg_name-version')."
            ),
        )
    pkg_name = filename_splitted[0]
    version = filename.removeprefix(f"{pkg_name}-").removesuffix(".tar.gz")
    return pkg_name, version


@router.post(
    "/collect/pixi/",
    status_code=202,
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

    settings = Inject(get_settings)
    # Check if Pixi is available
    if settings.pixi is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Pixi task collection is not available.",
        )
    # Check if provided Pixi version is available. Use default if not provided
    if pixi_version is None:
        pixi_version = settings.pixi.default_version
    else:
        if pixi_version not in settings.pixi.versions:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    f"Pixi version {pixi_version} is not available. Available"
                    f"versions: {list(settings.pixi.versions.keys())}"
                ),
            )

    pkg_name, version = get_pkgname_and_version(file.filename)
