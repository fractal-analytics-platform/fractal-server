import os
import shlex
import subprocess  # nosec
from pathlib import Path

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status
from sqlalchemy.ext.asyncio import AsyncSession

from ._aux_functions_tasks import _get_valid_user_group_id
from ._aux_functions_tasks import _verify_non_duplication_group_constraint
from ._aux_functions_tasks import _verify_non_duplication_user_constraint
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.routes.auth import current_active_verified_user
from fractal_server.app.schemas.v2 import TaskCollectCustomV2
from fractal_server.app.schemas.v2 import TaskCreateV2
from fractal_server.app.schemas.v2 import TaskGroupCreateV2
from fractal_server.app.schemas.v2 import TaskGroupV2OriginEnum
from fractal_server.app.schemas.v2 import TaskReadV2
from fractal_server.config import get_settings
from fractal_server.logger import set_logger
from fractal_server.string_tools import validate_cmd
from fractal_server.syringe import Inject
from fractal_server.tasks.v2.utils_background import (
    _prepare_tasks_metadata,
)
from fractal_server.tasks.v2.utils_database import (
    create_db_tasks_and_update_task_group_async,
)

router = APIRouter()

logger = set_logger(__name__)


@router.post(
    "/collect/custom/", status_code=201, response_model=list[TaskReadV2]
)
async def collect_task_custom(
    task_collect: TaskCollectCustomV2,
    private: bool = False,
    user_group_id: int | None = None,
    user: UserOAuth = Depends(current_active_verified_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[TaskReadV2]:

    settings = Inject(get_settings)

    # Validate query parameters related to user-group ownership
    user_group_id = await _get_valid_user_group_id(
        user_group_id=user_group_id,
        private=private,
        user_id=user.id,
        db=db,
    )

    if settings.FRACTAL_RUNNER_BACKEND == "slurm_ssh":
        if task_collect.package_root is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Cannot infer 'package_root' with 'slurm_ssh' backend.",
            )
    else:
        if not os.access(
            task_collect.python_interpreter, os.X_OK
        ) or not os.access(task_collect.python_interpreter, os.R_OK):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    f"{task_collect.python_interpreter=} "
                    "is not accessible to the Fractal user "
                    "or it is not executable."
                ),
            )
        if not Path(task_collect.python_interpreter).is_file():
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"{task_collect.python_interpreter=} is not a file.",
            )
        if task_collect.package_root is not None:
            if not os.access(task_collect.package_root, os.R_OK):
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=(
                        f"{task_collect.package_root=} "
                        "is not accessible to the Fractal user."
                    ),
                )
            if not Path(task_collect.package_root).is_dir():
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=f"{task_collect.package_root=} is not a directory.",
                )

    if task_collect.package_root is None:

        package_name_underscore = task_collect.package_name.replace("-", "_")
        # Note that python_command is then used as part of a subprocess.run
        # statement: be careful with mixing `'` and `"`.
        validate_cmd(package_name_underscore)
        python_command = (
            "import importlib.util; "
            "from pathlib import Path; "
            "init_path=importlib.util.find_spec"
            f'("{package_name_underscore}").origin; '
            "print(Path(init_path).parent.as_posix())"
        )
        logger.debug(
            f"Now running {python_command=} through "
            f"{task_collect.python_interpreter}."
        )
        res = subprocess.run(  # nosec
            shlex.split(
                f"{task_collect.python_interpreter} -c '{python_command}'"
            ),
            capture_output=True,
            encoding="utf8",
        )

        if (
            res.returncode != 0
            or res.stdout is None
            or ("\n" in res.stdout.strip("\n"))
        ):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    "Cannot determine 'package_root'.\n"
                    f"Original output: {res.stdout}\n"
                    f"Original error: {res.stderr}"
                ),
            )
        package_root = Path(res.stdout.strip("\n"))
    else:
        package_root = Path(task_collect.package_root)

    task_list: list[TaskCreateV2] = _prepare_tasks_metadata(
        package_manifest=task_collect.manifest,
        python_bin=Path(task_collect.python_interpreter),
        package_root=package_root,
        package_version=task_collect.version,
    )

    # Prepare task-group attributes
    task_group_attrs = dict(
        origin=TaskGroupV2OriginEnum.OTHER,
        pkg_name=task_collect.label,
        user_id=user.id,
        user_group_id=user_group_id,
        version=task_collect.version,
    )
    TaskGroupCreateV2(**task_group_attrs)

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

    task_group = TaskGroupV2(**task_group_attrs)
    db.add(task_group)
    await db.commit()
    await db.refresh(task_group)
    db.expunge(task_group)

    task_group = await create_db_tasks_and_update_task_group_async(
        task_list=task_list,
        task_group_id=task_group.id,
        db=db,
    )

    logger.debug(
        f"Custom-environment task collection by user {user.email} completed, "
        f"for package {task_collect}"
    )

    return task_group.task_list
