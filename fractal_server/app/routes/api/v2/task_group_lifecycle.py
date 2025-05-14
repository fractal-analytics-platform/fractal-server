from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Request
from fastapi import Response
from fastapi import status

from ...aux.validate_user_settings import validate_user_settings
from ._aux_functions_task_lifecycle import check_no_ongoing_activity
from ._aux_functions_task_lifecycle import check_no_submitted_job
from ._aux_functions_tasks import _get_task_group_full_access
from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.routes.auth import current_active_user
from fractal_server.app.schemas.v2 import TaskGroupActivityActionV2
from fractal_server.app.schemas.v2 import TaskGroupActivityStatusV2
from fractal_server.app.schemas.v2 import TaskGroupActivityV2Read
from fractal_server.app.schemas.v2 import TaskGroupReadV2
from fractal_server.app.schemas.v2 import TaskGroupV2OriginEnum
from fractal_server.config import get_settings
from fractal_server.logger import set_logger
from fractal_server.ssh._fabric import SSHConfig
from fractal_server.syringe import Inject
from fractal_server.tasks.v2.local import deactivate_local
from fractal_server.tasks.v2.local import reactivate_local
from fractal_server.tasks.v2.ssh import deactivate_ssh
from fractal_server.tasks.v2.ssh import reactivate_ssh
from fractal_server.utils import get_timestamp

router = APIRouter()


logger = set_logger(__name__)


@router.post(
    "/{task_group_id}/deactivate/",
    response_model=TaskGroupActivityV2Read,
)
async def deactivate_task_group(
    task_group_id: int,
    background_tasks: BackgroundTasks,
    response: Response,
    request: Request,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> TaskGroupReadV2:
    """
    Deactivate task-group venv
    """
    # Check access
    task_group = await _get_task_group_full_access(
        task_group_id=task_group_id,
        user_id=user.id,
        db=db,
    )

    # Check no other activity is ongoing
    await check_no_ongoing_activity(task_group_id=task_group_id, db=db)

    # Check no submitted jobs use tasks from this task group
    await check_no_submitted_job(task_group_id=task_group.id, db=db)

    # Check that task-group is active
    if not task_group.active:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Cannot deactivate a task group with {task_group.active=}."
            ),
        )

    # Shortcut for task-group with origin="other"
    if task_group.origin == TaskGroupV2OriginEnum.OTHER:
        task_group.active = False
        task_group_activity = TaskGroupActivityV2(
            user_id=task_group.user_id,
            taskgroupv2_id=task_group.id,
            status=TaskGroupActivityStatusV2.OK,
            action=TaskGroupActivityActionV2.DEACTIVATE,
            pkg_name=task_group.pkg_name,
            version=(task_group.version or "N/A"),
            log=(
                f"Task group has {task_group.origin=}, set "
                "task_group.active to False and exit."
            ),
            timestamp_started=get_timestamp(),
            timestamp_ended=get_timestamp(),
        )
        db.add(task_group)
        db.add(task_group_activity)
        await db.commit()
        response.status_code = status.HTTP_202_ACCEPTED
        return task_group_activity

    task_group_activity = TaskGroupActivityV2(
        user_id=task_group.user_id,
        taskgroupv2_id=task_group.id,
        status=TaskGroupActivityStatusV2.PENDING,
        action=TaskGroupActivityActionV2.DEACTIVATE,
        pkg_name=task_group.pkg_name,
        version=task_group.version,
        timestamp_started=get_timestamp(),
    )
    task_group.active = False
    db.add(task_group)
    db.add(task_group_activity)
    await db.commit()

    # Submit background task
    settings = Inject(get_settings)
    if settings.FRACTAL_RUNNER_BACKEND == "slurm_ssh":

        # Validate user settings (backend-specific)
        user_settings = await validate_user_settings(
            user=user, backend=settings.FRACTAL_RUNNER_BACKEND, db=db
        )

        # User appropriate FractalSSH object
        ssh_config = SSHConfig(
            user=user_settings.ssh_username,
            host=user_settings.ssh_host,
            key_path=user_settings.ssh_private_key_path,
        )

        background_tasks.add_task(
            deactivate_ssh,
            task_group_id=task_group.id,
            task_group_activity_id=task_group_activity.id,
            ssh_config=ssh_config,
            tasks_base_dir=user_settings.ssh_tasks_dir,
        )

    else:
        background_tasks.add_task(
            deactivate_local,
            task_group_id=task_group.id,
            task_group_activity_id=task_group_activity.id,
        )

    logger.debug(
        "Task group deactivation endpoint: start deactivate "
        "and return task_group_activity"
    )
    response.status_code = status.HTTP_202_ACCEPTED
    return task_group_activity


@router.post(
    "/{task_group_id}/reactivate/",
    response_model=TaskGroupActivityV2Read,
)
async def reactivate_task_group(
    task_group_id: int,
    background_tasks: BackgroundTasks,
    response: Response,
    request: Request,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> TaskGroupReadV2:
    """
    Deactivate task-group venv
    """

    # Check access
    task_group = await _get_task_group_full_access(
        task_group_id=task_group_id,
        user_id=user.id,
        db=db,
    )

    # Check that task-group is not active
    if task_group.active:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Cannot reactivate a task group with {task_group.active=}."
            ),
        )

    # Check no other activity is ongoing
    await check_no_ongoing_activity(task_group_id=task_group_id, db=db)

    # Check no submitted jobs use tasks from this task group
    await check_no_submitted_job(task_group_id=task_group.id, db=db)

    # Shortcut for task-group with origin="other"
    if task_group.origin == TaskGroupV2OriginEnum.OTHER:
        task_group.active = True
        task_group_activity = TaskGroupActivityV2(
            user_id=task_group.user_id,
            taskgroupv2_id=task_group.id,
            status=TaskGroupActivityStatusV2.OK,
            action=TaskGroupActivityActionV2.REACTIVATE,
            pkg_name=task_group.pkg_name,
            version=(task_group.version or "N/A"),
            log=(
                f"Task group has {task_group.origin=}, set "
                "task_group.active to True and exit."
            ),
            timestamp_started=get_timestamp(),
            timestamp_ended=get_timestamp(),
        )
        db.add(task_group)
        db.add(task_group_activity)
        await db.commit()
        response.status_code = status.HTTP_202_ACCEPTED
        return task_group_activity

    if task_group.pip_freeze is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "Cannot reactivate a task group with "
                f"{task_group.pip_freeze=}."
            ),
        )

    task_group_activity = TaskGroupActivityV2(
        user_id=task_group.user_id,
        taskgroupv2_id=task_group.id,
        status=TaskGroupActivityStatusV2.PENDING,
        action=TaskGroupActivityActionV2.REACTIVATE,
        pkg_name=task_group.pkg_name,
        version=task_group.version,
        timestamp_started=get_timestamp(),
    )
    db.add(task_group_activity)
    await db.commit()

    # Submit background task
    settings = Inject(get_settings)
    if settings.FRACTAL_RUNNER_BACKEND == "slurm_ssh":

        # Validate user settings (backend-specific)
        user_settings = await validate_user_settings(
            user=user, backend=settings.FRACTAL_RUNNER_BACKEND, db=db
        )

        # Use appropriate SSH credentials
        ssh_config = SSHConfig(
            user=user_settings.ssh_username,
            host=user_settings.ssh_host,
            key_path=user_settings.ssh_private_key_path,
        )

        background_tasks.add_task(
            reactivate_ssh,
            task_group_id=task_group.id,
            task_group_activity_id=task_group_activity.id,
            ssh_config=ssh_config,
            tasks_base_dir=user_settings.ssh_tasks_dir,
        )

    else:
        background_tasks.add_task(
            reactivate_local,
            task_group_id=task_group.id,
            task_group_activity_id=task_group_activity.id,
        )
    logger.debug(
        "Task group reactivation endpoint: start reactivate "
        "and return task_group_activity"
    )
    response.status_code = status.HTTP_202_ACCEPTED
    return task_group_activity
