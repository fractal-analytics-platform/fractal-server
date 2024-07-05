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
from ....security import current_active_verified_user
from ....security import User
from fractal_server.tasks.v2.background_operations import _insert_tasks
from fractal_server.tasks.v2.background_operations import (
    _prepare_tasks_metadata,
)


router = APIRouter()

logger = set_logger(__name__)


@router.post("/collect/custom/", status_code=201)
async def collect_task_custom(
    task_collect: TaskCollectCustomV2,
    user: User = Depends(current_active_verified_user),
    db: DBSyncSession = Depends(get_sync_db),
):

    settings = Inject(get_settings)

    if task_collect.package_root is None:
        if task_collect.package_name is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    "Must provide at least one of 'package_root' "
                    "and 'package_name'."
                ),
            )
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
                    f"Package '{task_collect.package_name}' not installed at "
                    f"{task_collect.python_interpreter}."
                ),
            )
        package_root_dir = next(
            (
                it.split()[1]
                for it in res.stdout.split("\n")
                if it.startswith("Location")
            ),
            None,
        )
        if package_root_dir is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    "Command 'pip show' gave an unexpected response:\n"
                    "the output should contain 'Location /path/to/package', "
                    f"instead returned: {res.stdout}"
                ),
            )
        package_root = Path(f"{package_root_dir}/{task_collect.package_name}")
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

    task_list = _prepare_tasks_metadata(
        package_manifest=task_collect.manifest,
        package_source=f"{owner}:{task_collect.source}",
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
    if res.scalars().all():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "Some sources already used by some TaskV2: "
                f"{res.scalars().all()}"
            ),
        )
    stm = select(TaskV1).where(TaskV1.source.in_(sources))
    res = db.execute(stm)
    if res.scalars().all():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "Some sources already used by some TaskV1: "
                f"{res.scalars().all()}"
            ),
        )
    _insert_tasks(task_list=task_list, db=db)
