import os
from pathlib import Path

from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import Depends
from fastapi import Form
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from fastapi import UploadFile

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.routes.api.v2._aux_functions_tasks import (
    _get_valid_user_group_id,
)
from fractal_server.app.routes.api.v2._aux_functions_tasks import (
    _verify_non_duplication_group_constraint,
)
from fractal_server.app.routes.api.v2._aux_functions_tasks import (
    _verify_non_duplication_group_path,
)
from fractal_server.app.routes.api.v2._aux_functions_tasks import (
    _verify_non_duplication_user_constraint,
)
from fractal_server.app.routes.auth import current_active_verified_user
from fractal_server.app.routes.aux.validate_user_settings import (
    validate_user_settings,
)
from fractal_server.app.schemas.v2 import FractalUploadedFile
from fractal_server.app.schemas.v2 import TaskGroupActivityActionV2
from fractal_server.app.schemas.v2 import TaskGroupActivityStatusV2
from fractal_server.app.schemas.v2 import TaskGroupActivityV2Read
from fractal_server.app.schemas.v2.task_group import TaskGroupV2OriginEnum
from fractal_server.config import get_settings
from fractal_server.logger import set_logger
from fractal_server.ssh._fabric import SSHConfig
from fractal_server.syringe import Inject
from fractal_server.tasks.v2.local import collect_local_pixi
from fractal_server.tasks.v2.ssh import collect_ssh_pixi
from fractal_server.tasks.v2.utils_package_names import normalize_package_name
from fractal_server.types import NonEmptyStr


router = APIRouter()

logger = set_logger(__name__)


def validate_pkgname_and_version(filename: str) -> tuple[str, str]:
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
                f"Invalid filename: '{filename}' must contain a single `-` "
                "character, separating the package name from the version "
                "(expected format: 'pkg_name-version')."
            ),
        )

    pkg_name = filename_splitted[0]
    version = filename.removeprefix(f"{pkg_name}-").removesuffix(".tar.gz")

    return normalize_package_name(pkg_name), version


@router.post(
    "/collect/pixi/",
    status_code=202,
    response_model=TaskGroupActivityV2Read,
)
async def collect_task_pixi(
    response: Response,
    background_tasks: BackgroundTasks,
    file: UploadFile,
    pixi_version: NonEmptyStr | None = Form(None),
    private: bool = False,
    user_group_id: int | None = None,
    user: UserOAuth = Depends(current_active_verified_user),
    db: AsyncSession = Depends(get_async_db),
) -> TaskGroupActivityV2Read:
    settings = Inject(get_settings)
    # Check if Pixi is available
    if settings.pixi is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
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
                    f"Pixi version {pixi_version} is not available. Available "
                    f"versions: {list(settings.pixi.versions.keys())}"
                ),
            )

    pkg_name, version = validate_pkgname_and_version(file.filename)
    tar_gz_content = await file.read()
    tar_gz_file = FractalUploadedFile(
        filename=file.filename,
        contents=tar_gz_content,
    )

    user_group_id = await _get_valid_user_group_id(
        user_group_id=user_group_id,
        private=private,
        user_id=user.id,
        db=db,
    )

    user_settings = await validate_user_settings(
        user=user, backend=settings.FRACTAL_RUNNER_BACKEND, db=db
    )

    if settings.FRACTAL_RUNNER_BACKEND == "slurm_ssh":
        base_tasks_path = user_settings.ssh_tasks_dir
    else:
        base_tasks_path = settings.FRACTAL_TASKS_DIR.as_posix()
    task_group_path = (
        Path(base_tasks_path) / str(user.id) / pkg_name / version
    ).as_posix()

    task_group_attrs = dict(
        user_id=user.id,
        user_group_id=user_group_id,
        origin=TaskGroupV2OriginEnum.PIXI,
        pixi_version=pixi_version,
        pkg_name=pkg_name,
        version=version,
        path=task_group_path,
    )

    await _verify_non_duplication_user_constraint(
        user_id=user.id,
        pkg_name=task_group_attrs["pkg_name"],
        version=task_group_attrs["version"],
        db=db,
    )
    await _verify_non_duplication_group_constraint(
        user_group_id=task_group_attrs["user_group_id"],
        pkg_name=task_group_attrs["pkg_name"],
        version=task_group_attrs["version"],
        db=db,
    )
    await _verify_non_duplication_group_path(
        path=task_group_attrs["path"],
        db=db,
    )

    if settings.FRACTAL_RUNNER_BACKEND != "slurm_ssh":
        if Path(task_group_path).exists():
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"{task_group_path} already exists.",
            )

    task_group = TaskGroupV2(**task_group_attrs)
    db.add(task_group)
    await db.commit()
    await db.refresh(task_group)
    db.expunge(task_group)

    task_group_activity = TaskGroupActivityV2(
        user_id=task_group.user_id,
        taskgroupv2_id=task_group.id,
        status=TaskGroupActivityStatusV2.PENDING,
        action=TaskGroupActivityActionV2.COLLECT,
        pkg_name=task_group.pkg_name,
        version=task_group.version,
    )
    db.add(task_group_activity)
    await db.commit()
    await db.refresh(task_group_activity)

    if settings.FRACTAL_RUNNER_BACKEND == "slurm_ssh":
        ssh_config = SSHConfig(
            user=user_settings.ssh_username,
            host=user_settings.ssh_host,
            key_path=user_settings.ssh_private_key_path,
        )

        background_tasks.add_task(
            collect_ssh_pixi,
            task_group_id=task_group.id,
            task_group_activity_id=task_group_activity.id,
            ssh_config=ssh_config,
            tasks_base_dir=user_settings.ssh_tasks_dir,
            tar_gz_file=tar_gz_file,
        )
    else:
        background_tasks.add_task(
            collect_local_pixi,
            task_group_id=task_group.id,
            task_group_activity_id=task_group_activity.id,
            tar_gz_file=tar_gz_file,
        )
    logger.info(
        "Task-collection endpoint: start background collection "
        "and return task_group_activity. "
        f"Current pid is {os.getpid()}. "
    )
    response.status_code = status.HTTP_202_ACCEPTED
    return task_group_activity
