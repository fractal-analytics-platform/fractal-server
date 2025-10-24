from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status

from ._aux_functions_task_lifecycle import check_no_ongoing_activity
from ._aux_functions_task_lifecycle import check_no_related_workflowtask
from ._aux_functions_task_lifecycle import check_no_submitted_job
from ._aux_functions_tasks import _get_task_group_full_access
from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.routes.auth import current_user_act_ver_prof
from fractal_server.app.routes.aux.validate_user_profile import (
    validate_user_profile,
)
from fractal_server.app.schemas.v2 import ResourceType
from fractal_server.app.schemas.v2 import TaskGroupActivityActionV2
from fractal_server.app.schemas.v2 import TaskGroupActivityStatusV2
from fractal_server.app.schemas.v2 import TaskGroupActivityV2Read
from fractal_server.app.schemas.v2 import TaskGroupReadV2
from fractal_server.app.schemas.v2 import TaskGroupV2OriginEnum
from fractal_server.logger import set_logger
from fractal_server.tasks.v2.local import deactivate_local
from fractal_server.tasks.v2.local import deactivate_local_pixi
from fractal_server.tasks.v2.local import delete_local
from fractal_server.tasks.v2.local import reactivate_local
from fractal_server.tasks.v2.local import reactivate_local_pixi
from fractal_server.tasks.v2.ssh import deactivate_ssh
from fractal_server.tasks.v2.ssh import deactivate_ssh_pixi
from fractal_server.tasks.v2.ssh import delete_ssh
from fractal_server.tasks.v2.ssh import reactivate_ssh
from fractal_server.tasks.v2.ssh import reactivate_ssh_pixi
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
    user: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
) -> TaskGroupActivityV2Read:
    """
    Deactivate task-group venv
    """

    # Get validated resource and profile
    resource, profile = await validate_user_profile(user=user, db=db)

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
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
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
    if resource.type == ResourceType.SLURM_SSH:
        if task_group.origin == TaskGroupV2OriginEnum.PIXI:
            deactivate_function = deactivate_ssh_pixi
        else:
            deactivate_function = deactivate_ssh
    else:
        if task_group.origin == TaskGroupV2OriginEnum.PIXI:
            deactivate_function = deactivate_local_pixi
        else:
            deactivate_function = deactivate_local
    background_tasks.add_task(
        deactivate_function,
        task_group_id=task_group.id,
        task_group_activity_id=task_group_activity.id,
        resource=resource,
        profile=profile,
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
    user: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
) -> TaskGroupReadV2:
    """
    Deactivate task-group venv
    """
    # Get validated resource and profile
    resource, profile = await validate_user_profile(user=user, db=db)

    # Check access
    task_group = await _get_task_group_full_access(
        task_group_id=task_group_id,
        user_id=user.id,
        db=db,
    )

    # Check that task-group is not active
    if task_group.active:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
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

    if task_group.env_info is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                "Cannot reactivate a task group with "
                f"{task_group.env_info=}."
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
    if resource.type == ResourceType.SLURM_SSH:
        if task_group.origin == TaskGroupV2OriginEnum.PIXI:
            reactivate_function = reactivate_ssh_pixi
        else:
            reactivate_function = reactivate_ssh
    else:
        if task_group.origin == TaskGroupV2OriginEnum.PIXI:
            reactivate_function = reactivate_local_pixi
        else:
            reactivate_function = reactivate_local
    background_tasks.add_task(
        reactivate_function,
        task_group_id=task_group.id,
        task_group_activity_id=task_group_activity.id,
        resource=resource,
        profile=profile,
    )
    logger.debug(
        "Task group reactivation endpoint: start reactivate "
        "and return task_group_activity"
    )
    response.status_code = status.HTTP_202_ACCEPTED
    return task_group_activity


@router.post(
    "/{task_group_id}/delete/",
    status_code=202,
)
async def delete_task_group(
    task_group_id: int,
    background_tasks: BackgroundTasks,
    response: Response,
    user: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
) -> TaskGroupActivityV2Read:
    """
    Deletion of task-group from db and file system
    """
    # Get validated resource and profile
    resource, profile = await validate_user_profile(user=user, db=db)

    task_group = await _get_task_group_full_access(
        task_group_id=task_group_id,
        user_id=user.id,
        db=db,
    )
    await check_no_ongoing_activity(task_group_id=task_group_id, db=db)
    await check_no_related_workflowtask(task_group=task_group, db=db)

    task_group_activity = TaskGroupActivityV2(
        user_id=task_group.user_id,
        taskgroupv2_id=task_group.id,
        status=TaskGroupActivityStatusV2.PENDING,
        action=TaskGroupActivityActionV2.DELETE,
        pkg_name=task_group.pkg_name,
        version=(task_group.version or "N/A"),
        timestamp_started=get_timestamp(),
    )
    db.add(task_group_activity)
    await db.commit()

    if resource.type == ResourceType.SLURM_SSH:
        delete_function = delete_ssh
    else:
        delete_function = delete_local

    background_tasks.add_task(
        delete_function,
        task_group_id=task_group.id,
        task_group_activity_id=task_group_activity.id,
        resource=resource,
        profile=profile,
    )

    response.status_code = status.HTTP_202_ACCEPTED
    return task_group_activity
