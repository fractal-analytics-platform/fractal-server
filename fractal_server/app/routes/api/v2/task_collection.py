from pathlib import Path
from typing import Optional

from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Request
from fastapi import Response
from fastapi import status
from pydantic import ValidationError
from sqlmodel import select

from .....config import get_settings
from .....logger import reset_logger_handlers
from .....logger import set_logger
from .....syringe import Inject
from ....db import AsyncSession
from ....db import get_async_db
from ....models.v2 import TaskGroupV2
from ....schemas.v2 import TaskCollectPipV2
from ....schemas.v2 import TaskGroupActivityStatusV2
from ....schemas.v2 import TaskGroupActivityV2Read
from ....schemas.v2 import TaskGroupCreateV2Strict
from ...aux.validate_user_settings import validate_user_settings
from ._aux_functions_task_lifecycle import get_package_version_from_pypi
from ._aux_functions_tasks import _get_valid_user_group_id
from ._aux_functions_tasks import _verify_non_duplication_group_constraint
from ._aux_functions_tasks import _verify_non_duplication_user_constraint
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.routes.auth import current_active_verified_user
from fractal_server.app.schemas.v2 import (
    TaskGroupActivityActionV2,
)
from fractal_server.app.schemas.v2 import TaskGroupV2OriginEnum
from fractal_server.tasks.v2.local.collect import (
    collect_local,
)
from fractal_server.tasks.v2.ssh import collect_ssh
from fractal_server.tasks.v2.utils_package_names import _parse_wheel_filename
from fractal_server.tasks.v2.utils_package_names import normalize_package_name
from fractal_server.tasks.v2.utils_python_interpreter import (
    get_python_interpreter_v2,
)

router = APIRouter()

logger = set_logger(__name__)


@router.post(
    "/collect/pip/",
    response_model=TaskGroupActivityV2Read,
)
async def collect_tasks_pip(
    task_collect: TaskCollectPipV2,
    background_tasks: BackgroundTasks,
    response: Response,
    request: Request,
    private: bool = False,
    user_group_id: Optional[int] = None,
    user: UserOAuth = Depends(current_active_verified_user),
    db: AsyncSession = Depends(get_async_db),
) -> TaskGroupActivityV2Read:
    """
    Task-collection endpoint
    """

    # Get settings
    settings = Inject(get_settings)

    # Initialize task-group attributes
    task_group_attrs = dict(user_id=user.id)

    # Set/check python version
    if task_collect.python_version is None:
        task_group_attrs[
            "python_version"
        ] = settings.FRACTAL_TASKS_PYTHON_DEFAULT_VERSION
    else:
        task_group_attrs["python_version"] = task_collect.python_version
    try:
        get_python_interpreter_v2(
            python_version=task_group_attrs["python_version"]
        )
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Python version {task_group_attrs['python_version']} is "
                "not available for Fractal task collection."
            ),
        )

    # Set pip_extras
    if task_collect.package_extras is not None:
        task_group_attrs["pip_extras"] = task_collect.package_extras

    # Set pinned_package_versions
    if task_collect.pinned_package_versions is not None:
        task_group_attrs[
            "pinned_package_versions"
        ] = task_collect.pinned_package_versions

    # Set pkg_name, version, origin and wheel_path
    if task_collect.package.endswith(".whl"):
        try:
            task_group_attrs["wheel_path"] = task_collect.package
            wheel_filename = Path(task_group_attrs["wheel_path"]).name
            wheel_info = _parse_wheel_filename(wheel_filename)
        except ValueError as e:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    f"Invalid wheel-file name {wheel_filename}. "
                    f"Original error: {str(e)}",
                ),
            )
        task_group_attrs["pkg_name"] = normalize_package_name(
            wheel_info["distribution"]
        )
        task_group_attrs["version"] = wheel_info["version"]
        task_group_attrs["origin"] = TaskGroupV2OriginEnum.WHEELFILE
    else:
        pkg_name = task_collect.package
        task_group_attrs["pkg_name"] = normalize_package_name(pkg_name)
        task_group_attrs["origin"] = TaskGroupV2OriginEnum.PYPI
        latest_version = await get_package_version_from_pypi(
            task_collect.package,
            task_collect.package_version,
        )
        task_group_attrs["version"] = latest_version

    # Validate query parameters related to user-group ownership
    user_group_id = await _get_valid_user_group_id(
        user_group_id=user_group_id,
        private=private,
        user_id=user.id,
        db=db,
    )

    # Set user_group_id
    task_group_attrs["user_group_id"] = user_group_id

    # Validate user settings (backend-specific)
    user_settings = await validate_user_settings(
        user=user, backend=settings.FRACTAL_RUNNER_BACKEND, db=db
    )

    # Set path and venv_path
    if settings.FRACTAL_RUNNER_BACKEND == "slurm_ssh":
        base_tasks_path = user_settings.ssh_tasks_dir
    else:
        base_tasks_path = settings.FRACTAL_TASKS_DIR.as_posix()
    task_group_path = (
        Path(base_tasks_path)
        / str(user.id)
        / task_group_attrs["pkg_name"]
        / task_group_attrs["version"]
    ).as_posix()
    task_group_attrs["path"] = task_group_path
    task_group_attrs["venv_path"] = Path(task_group_path, "venv").as_posix()

    # Validate TaskGroupV2 attributes
    try:
        TaskGroupCreateV2Strict(**task_group_attrs)
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid task-group object. Original error: {e}",
        )

    # Database checks

    # Verify non-duplication constraints
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

    # Verify that task-group path is unique
    stm = select(TaskGroupV2).where(TaskGroupV2.path == task_group_path)
    res = await db.execute(stm)
    for conflicting_task_group in res.scalars().all():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Another task-group already has path={task_group_path}.\n"
                f"{conflicting_task_group=}"
            ),
        )

    # On-disk checks

    if settings.FRACTAL_RUNNER_BACKEND != "slurm_ssh":

        # Verify that folder does not exist (for local collection)
        if Path(task_group_path).exists():
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"{task_group_path} already exists.",
            )

        # Verify that wheel file exists
        wheel_path = task_group_attrs.get("wheel_path", None)
        if wheel_path is not None:
            if not Path(wheel_path).exists():
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=f"No such file: {wheel_path}.",
                )

    # Create TaskGroupV2 object
    task_group = TaskGroupV2(**task_group_attrs)
    db.add(task_group)
    await db.commit()
    await db.refresh(task_group)
    db.expunge(task_group)

    # All checks are OK, proceed with task collection
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
    logger = set_logger(logger_name="collect_tasks_pip")

    # END of SSH/non-SSH common part

    if settings.FRACTAL_RUNNER_BACKEND == "slurm_ssh":
        # SSH task collection
        # Use appropriate FractalSSH object
        ssh_credentials = dict(
            user=user_settings.ssh_username,
            host=user_settings.ssh_host,
            key_path=user_settings.ssh_private_key_path,
        )
        fractal_ssh_list = request.app.state.fractal_ssh_list
        fractal_ssh = fractal_ssh_list.get(**ssh_credentials)

        background_tasks.add_task(
            collect_ssh,
            task_group_id=task_group.id,
            task_group_activity_id=task_group_activity.id,
            fractal_ssh=fractal_ssh,
            tasks_base_dir=user_settings.ssh_tasks_dir,
        )

    else:
        # Local task collection
        background_tasks.add_task(
            collect_local,
            task_group_id=task_group.id,
            task_group_activity_id=task_group_activity.id,
        )
    logger.debug(
        "Task-collection endpoint: start background collection "
        "and return task_group_activity"
    )
    reset_logger_handlers(logger)
    response.status_code = status.HTTP_202_ACCEPTED
    return task_group_activity
