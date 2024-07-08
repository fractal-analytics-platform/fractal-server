import shlex
import subprocess  # nosec
from pathlib import Path

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status
from sqlmodel import select

from .....config import get_settings
from .....logger import set_logger
from .....syringe import Inject
from ....db import DBSyncSession
from ....db import get_sync_db
from ....models.v1 import Task as TaskV1
from ....models.v2 import TaskV2
from ....schemas.v2 import TaskCollectCustomV2
from ....schemas.v2 import TaskCreateV2
from ....schemas.v2 import TaskReadV2
from ....security import current_active_verified_user
from ....security import User
from fractal_server.tasks.v2.background_operations import _insert_tasks
from fractal_server.tasks.v2.background_operations import (
    _prepare_tasks_metadata,
)


router = APIRouter()

logger = set_logger(__name__)


@router.post(
    "/collect/custom/", status_code=201, response_model=list[TaskReadV2]
)
async def collect_task_custom(
    task_collect: TaskCollectCustomV2,
    user: User = Depends(current_active_verified_user),
    db: DBSyncSession = Depends(get_sync_db),
) -> list[TaskReadV2]:

    settings = Inject(get_settings)

    if task_collect.package_root is None:

        if settings.FRACTAL_RUNNER_BACKEND == "slurm_ssh":
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Cannot infer 'package_root' with 'slurm_ssh' backend.",
            )
        res = subprocess.run(  # nosec
            shlex.split(
                f"{task_collect.python_interpreter} "
                f"-m pip show {task_collect.package_name}"
            ),
            capture_output=True,
            encoding="utf8",
        )
        if res.returncode != 0:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    "Cannot determine 'package_root'.\n"
                    f"Original output: {res.stdout}\n"
                    f"Original error: {res.stderr}"
                ),
            )
        try:
            package_root_dir = next(
                it.split()[1]
                for it in res.stdout.split("\n")
                if it.startswith("Location")
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    "Command 'pip show' gave an unexpected response:\n"
                    "the output should contain 'Location /path/to/package', "
                    f"instead returned: {res.stdout}.\n"
                    f"Original error: {str(e)}"
                ),
            )
        package_root = Path(package_root_dir) / task_collect.package_name
    else:
        package_root = Path(task_collect.package_root)

    # Set task.owner attribute
    owner = user.username or user.slurm_user
    if owner is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "Cannot add a new task because current user does not "
                "have `username` or `slurm_user` attributes."
            ),
        )
    source = f"{owner}:{task_collect.source}"

    task_list: list[TaskCreateV2] = _prepare_tasks_metadata(
        package_manifest=task_collect.manifest,
        package_source=source,
        python_bin=Path(task_collect.python_interpreter),
        package_root=package_root,
        package_version=task_collect.version,
    )
    # Verify that source is not already in use (note: this check is only useful
    # to provide a user-friendly error message, but `task.source` uniqueness is
    # already guaranteed by a constraint in the table definition).
    sources = [task.source for task in task_list]
    stm = select(TaskV2).where(TaskV2.source.in_(sources))
    res = db.execute(stm)
    overlapping_sources_v2 = res.scalars().all()
    if overlapping_sources_v2:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "Some sources already used by some TaskV2: "
                f"{overlapping_sources_v2}"
            ),
        )
    stm = select(TaskV1).where(TaskV1.source.in_(sources))
    res = db.execute(stm)
    overlapping_sources_v1 = res.scalars().all()
    if overlapping_sources_v1:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "Some sources already used by some TaskV1: "
                f"{overlapping_sources_v1}"
            ),
        )

    task_list_db: list[TaskV2] = _insert_tasks(
        task_list=task_list, owner=owner, db=db
    )

    logger.debug(
        f"Custom-environment task collection by user {user.email} completed, "
        f"for package with {source=}"
    )

    return task_list_db
