import json
from pathlib import Path

from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import Depends
from fastapi import File
from fastapi import Form
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from fastapi import UploadFile
from pydantic import BaseModel
from pydantic import model_validator
from pydantic import ValidationError

from .....config import get_settings
from .....logger import reset_logger_handlers
from .....logger import set_logger
from .....syringe import Inject
from ....db import AsyncSession
from ....db import get_async_db
from ....models.v2 import TaskGroupV2
from ....schemas.v2 import FractalUploadedFile
from ....schemas.v2 import TaskCollectPipV2
from ....schemas.v2 import TaskGroupActivityStatusV2
from ....schemas.v2 import TaskGroupActivityV2Read
from ....schemas.v2 import TaskGroupCreateV2Strict
from ...aux.validate_user_settings import validate_user_settings
from ._aux_functions_task_lifecycle import get_package_version_from_pypi
from ._aux_functions_tasks import _get_valid_user_group_id
from ._aux_functions_tasks import _verify_non_duplication_group_constraint
from ._aux_functions_tasks import _verify_non_duplication_group_path
from ._aux_functions_tasks import _verify_non_duplication_user_constraint
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.routes.auth import current_active_verified_user
from fractal_server.app.schemas.v2 import (
    TaskGroupActivityActionV2,
)
from fractal_server.app.schemas.v2 import TaskGroupV2OriginEnum
from fractal_server.ssh._fabric import SSHConfig
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

FORBIDDEN_CHAR_WHEEL = [";", "/"]


class CollectionRequestData(BaseModel):
    """
    Validate form data _and_ wheel file.
    """

    task_collect: TaskCollectPipV2
    file: UploadFile | None = None
    origin: TaskGroupV2OriginEnum

    @model_validator(mode="before")
    @classmethod
    def validate_data(cls, values):
        file = values.get("file")
        package = values.get("task_collect").package
        package_version = values.get("task_collect").package_version

        if file is None:
            if package is None:
                raise ValueError(
                    "When no `file` is provided, `package` is required."
                )
            values["origin"] = TaskGroupV2OriginEnum.PYPI
        else:
            if package is not None:
                raise ValueError(
                    "Cannot set `package` when `file` is provided "
                    f"(given package='{package}')."
                )
            if package_version is not None:
                raise ValueError(
                    "Cannot set `package_version` when `file` is "
                    f"provided (given package_version='{package_version}')."
                )
            values["origin"] = TaskGroupV2OriginEnum.WHEELFILE

            for forbidden_char in FORBIDDEN_CHAR_WHEEL:
                if forbidden_char in file.filename:
                    raise ValueError(
                        "Wheel filename has forbidden characters, "
                        f"{FORBIDDEN_CHAR_WHEEL}"
                    )

        return values


def parse_request_data(
    package: str | None = Form(None),
    package_version: str | None = Form(None),
    package_extras: str | None = Form(None),
    python_version: str | None = Form(None),
    pinned_package_versions: str | None = Form(None),
    file: UploadFile | None = File(None),
) -> CollectionRequestData:
    """
    Expand the parsing/validation of `parse_form_data`, based on `file`.
    """

    try:
        # Convert dict_pinned_pkg from a JSON string into a Python dictionary
        dict_pinned_pkg = (
            json.loads(pinned_package_versions)
            if pinned_package_versions
            else None
        )
        # Validate and coerce form data
        task_collect_pip = TaskCollectPipV2(
            package=package,
            package_version=package_version,
            package_extras=package_extras,
            python_version=python_version,
            pinned_package_versions=dict_pinned_pkg,
        )

        data = CollectionRequestData(
            task_collect=task_collect_pip,
            file=file,
        )

    except (ValidationError, json.JSONDecodeError) as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid request-body\n{str(e)}",
        )

    return data


@router.post(
    "/collect/pip/",
    response_model=TaskGroupActivityV2Read,
)
async def collect_tasks_pip(
    response: Response,
    background_tasks: BackgroundTasks,
    request_data: CollectionRequestData = Depends(parse_request_data),
    private: bool = False,
    user_group_id: int | None = None,
    user: UserOAuth = Depends(current_active_verified_user),
    db: AsyncSession = Depends(get_async_db),
) -> TaskGroupActivityV2Read:
    """
    Task-collection endpoint
    """
    # Get settings
    settings = Inject(get_settings)

    # Get some validated request data
    task_collect = request_data.task_collect

    # Initialize task-group attributes
    task_group_attrs = dict(
        user_id=user.id,
        origin=request_data.origin,
    )

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

    # Initialize wheel_file_content as None
    wheel_file = None

    # Set pkg_name, version, origin and archive_path
    if request_data.origin == TaskGroupV2OriginEnum.WHEELFILE:
        try:
            wheel_filename = request_data.file.filename
            wheel_info = _parse_wheel_filename(wheel_filename)
            wheel_file_content = await request_data.file.read()
            wheel_file = FractalUploadedFile(
                filename=wheel_filename,
                contents=wheel_file_content,
            )
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
    elif request_data.origin == TaskGroupV2OriginEnum.PYPI:
        pkg_name = task_collect.package
        task_group_attrs["pkg_name"] = normalize_package_name(pkg_name)
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
    await _verify_non_duplication_group_path(
        path=task_group_attrs["path"],
        db=db,
    )

    # On-disk checks

    if settings.FRACTAL_RUNNER_BACKEND != "slurm_ssh":
        # Verify that folder does not exist (for local collection)
        if Path(task_group_path).exists():
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"{task_group_path} already exists.",
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
        ssh_config = SSHConfig(
            user=user_settings.ssh_username,
            host=user_settings.ssh_host,
            key_path=user_settings.ssh_private_key_path,
        )

        background_tasks.add_task(
            collect_ssh,
            task_group_id=task_group.id,
            task_group_activity_id=task_group_activity.id,
            ssh_config=ssh_config,
            tasks_base_dir=user_settings.ssh_tasks_dir,
            wheel_file=wheel_file,
        )

    else:
        # Local task collection

        background_tasks.add_task(
            collect_local,
            task_group_id=task_group.id,
            task_group_activity_id=task_group_activity.id,
            wheel_file=wheel_file,
        )
    logger.debug(
        "Task-collection endpoint: start background collection "
        "and return task_group_activity"
    )
    reset_logger_handlers(logger)
    response.status_code = status.HTTP_202_ACCEPTED
    return task_group_activity
