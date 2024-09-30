import shlex
import subprocess  # nosec
from pathlib import Path

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status
from sqlmodel import select

from ...auth._aux_auth import _get_default_user_group_id
from ...auth._aux_auth import _verify_user_belongs_to_group
from fractal_server.app.db import DBSyncSession
from fractal_server.app.db import get_sync_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v1 import Task as TaskV1
from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.routes.auth import current_active_verified_user
from fractal_server.app.routes.aux.validate_user_settings import (
    verify_user_has_settings,
)
from fractal_server.app.schemas.v2 import TaskCollectCustomV2
from fractal_server.app.schemas.v2 import TaskCreateV2
from fractal_server.app.schemas.v2 import TaskReadV2
from fractal_server.config import get_settings
from fractal_server.logger import set_logger
from fractal_server.string_tools import validate_cmd
from fractal_server.syringe import Inject
from fractal_server.tasks.v2.background_operations import (
    _prepare_tasks_metadata,
)
from fractal_server.tasks.v2.database_operations import (
    create_db_task_group_and_tasks,
)

router = APIRouter()

logger = set_logger(__name__)


@router.post(
    "/collect/custom/", status_code=201, response_model=list[TaskReadV2]
)
async def collect_task_custom(
    task_collect: TaskCollectCustomV2,
    user: UserOAuth = Depends(current_active_verified_user),
    db: DBSyncSession = Depends(get_sync_db),
) -> list[TaskReadV2]:

    settings = Inject(get_settings)

    if settings.FRACTAL_RUNNER_BACKEND == "slurm_ssh":
        if task_collect.package_root is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Cannot infer 'package_root' with 'slurm_ssh' backend.",
            )
    else:
        if not Path(task_collect.python_interpreter).is_file():
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    f"{task_collect.python_interpreter=} "
                    "doesn't exist or is not a file."
                ),
            )
        if (
            task_collect.package_root is not None
            and not Path(task_collect.package_root).is_dir()
        ):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    f"{task_collect.package_root=} "
                    "doesn't exist or is not a directory."
                ),
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

    # Set task.owner attribute
    if user.username:
        owner = user.username
    else:
        verify_user_has_settings(user)
        owner = user.settings.slurm_user
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
        overlapping_tasks_v2_source_and_id = [
            f"TaskV2 with ID {task.id} already has source='{task.source}'"
            for task in overlapping_sources_v2
        ]
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="\n".join(overlapping_tasks_v2_source_and_id),
        )
    stm = select(TaskV1).where(TaskV1.source.in_(sources))
    res = db.execute(stm)
    overlapping_sources_v1 = res.scalars().all()
    if overlapping_sources_v1:
        overlapping_tasks_v1_source_and_id = [
            f"TaskV1 with ID {task.id} already has source='{task.source}'\n"
            for task in overlapping_sources_v1
        ]
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="\n".join(overlapping_tasks_v1_source_and_id),
        )

    # Get default-user-group id # FIXME: let the user specify a group
    user_group_id = await _get_default_user_group_id()

    # Check current user belongs to group
    if user_group_id is not None:
        await _verify_user_belongs_to_group(
            user_id=user.id, user_group_id=user_group_id, db=db
        )

    task_group = create_db_task_group_and_tasks(
        task_list=task_list,
        task_group_dict={},  # FIXME
        user_id=current_active_verified_user.id,
        user_group_id=user_group_id,
        db=db,
    )

    logger.debug(
        f"Custom-environment task collection by user {user.email} completed, "
        f"for package with {source=}"
    )

    return task_group.task_list
