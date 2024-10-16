from typing import Optional

from devtools import debug
from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status
from sqlmodel import or_
from sqlmodel import select

from ....db import AsyncSession
from ....db import get_async_db
from ....models.v2 import TaskV2
from ....models.v2 import WorkflowV2
from ....schemas.v2 import TaskImportV2Legacy
from ....schemas.v2 import WorkflowImportV2
from ....schemas.v2 import WorkflowReadV2
from ....schemas.v2 import WorkflowTaskCreateV2
from ._aux_functions import _check_workflow_exists
from ._aux_functions import _get_project_check_owner
from ._aux_functions import _workflow_insert_task
from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.security import UserGroup
from fractal_server.app.models.v2.task import TaskGroupV2
from fractal_server.app.routes.auth import current_active_user
from fractal_server.app.security import FRACTAL_DEFAULT_GROUP_NAME

router = APIRouter()


async def _get_user_tasks(user_id: int, db: AsyncSession) -> list[TaskV2]:
    """Retrieve tasks that belong to the user."""
    debug("user task")
    stm = (
        select(TaskV2)
        .join(TaskGroupV2)
        .where(TaskGroupV2.id == TaskV2.taskgroupv2_id)
        .where(TaskGroupV2.user_id == user_id)
    )
    res = await db.execute(stm)
    return res.scalars().all()


async def _get_default_group_id(db: AsyncSession) -> int:
    """Get the default user group ID."""
    debug("group id")
    stm = select(UserGroup).where(UserGroup.name == FRACTAL_DEFAULT_GROUP_NAME)
    res = await db.execute(stm)
    return res.scalars().one().id


async def _find_task_by_source_or_version(
    wf_task, user, db, task_user_list, default_group_id
) -> Optional[int]:
    """Find a task by source or version."""
    if isinstance(wf_task.task, TaskImportV2Legacy):
        task = await _get_task_by_source(wf_task, user, db)
        debug("legacy with source")
    else:
        debug("new with version")
        task = await _get_task_by_version(
            wf_task, user, db, task_user_list, default_group_id
        )

    if not task:
        debug("noooooo")
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Expected one TaskV2 to match the imported one.",
        )

    return task.id


async def _get_task_by_source(wf_task, user, db) -> Optional[TaskV2]:
    """Find a task by its source."""
    if wf_task.task.source is None:
        return None

    stm_source = (
        select(TaskV2)
        .join(TaskGroupV2)
        .where(TaskGroupV2.id == TaskV2.taskgroupv2_id)
        .where(TaskV2.source == wf_task.task.source)
        .where(
            or_(
                TaskGroupV2.user_id == user.id,
                TaskGroupV2.user_group_id.in_(
                    select(LinkUserGroup.group_id).where(
                        LinkUserGroup.user_id == user.id
                    )
                ),
            )
        )
    )
    res = await db.execute(stm_source)
    return res.scalars().one_or_none()


async def _get_task_by_version(
    wf_task, user, db, task_user_list, default_group_id
) -> Optional[TaskV2]:
    """Find a task by version."""
    debug("into new with version")
    if wf_task.task.version is None:
        return await _get_latest_user_task(task_user_list, db, wf_task)

    stm = (
        select(TaskV2)
        .join(TaskGroupV2)
        .where(TaskGroupV2.id == TaskV2.taskgroupv2_id)
        .where(TaskGroupV2.pkg_name == wf_task.task.pkg_name)
        .where(TaskV2.name == wf_task.task.name)
        .where(TaskGroupV2.version == wf_task.task.version)
        .where(
            or_(
                TaskGroupV2.user_id == user.id,
                TaskGroupV2.user_group_id.in_(
                    select(LinkUserGroup.group_id).where(
                        LinkUserGroup.user_id == user.id
                    )
                ),
            )
        )
    )
    res = await db.execute(stm)
    task = res.scalars().one_or_none()

    if not task:
        return await _resolve_task_conflict(task, user, db, default_group_id)
    return task


async def _get_latest_user_task(task_user_list, db, wf_task) -> TaskV2:
    """Retrieve the latest task for the user."""
    if not task_user_list:
        debug("NO USERLIST????")
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Expected one TaskV2 to match the imported one.",
        )
    latest_task = max(task_user_list, key=lambda t: t.version)

    stm = (
        select(TaskV2)
        .join(TaskGroupV2)
        .where(TaskGroupV2.id == TaskV2.taskgroupv2_id)
        .where(TaskGroupV2.pkg_name == wf_task.task.pkg_name)
        .where(TaskV2.name == wf_task.task.name)
        .where(TaskGroupV2.version == latest_task.version)
    )
    debug(latest_task.version, wf_task.task.pkg_name, wf_task.task.name)
    debug("before query latest task")
    res = await db.execute(stm)
    debug(res.scalars().all())
    return res.scalars().one_or_none()


async def _resolve_task_conflict(
    task_list, user, db, default_group_id
) -> TaskV2:
    """Resolve conflicts when multiple tasks match."""
    debug("into resolve?")
    for task in task_list:
        if task.user_id == user.id or task.user_group_id == default_group_id:
            return task

    stm = (
        select(TaskV2)
        .join(TaskGroupV2)
        .join(LinkUserGroup)
        .where(LinkUserGroup.group_id == TaskGroupV2.user_group_id)
        .where(TaskGroupV2.id == TaskV2.taskgroupv2_id)
        .where(TaskGroupV2.user_group_id == task.user_group_id)
        .where(LinkUserGroup.user_id == user.id)
        .order_by(LinkUserGroup.timestamp_created.asc().limit(1))
    )
    res = await db.execute(stm)
    return res.scalars().one_or_none()


@router.post(
    "/project/{project_id}/workflow/import/",
    response_model=WorkflowReadV2,
    status_code=status.HTTP_201_CREATED,
)
async def import_workflow(
    project_id: int,
    workflow: WorkflowImportV2,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> WorkflowReadV2:
    """
    Import an existing workflow into a project and create required objects.
    """

    # Preliminary checks
    await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )
    await _check_workflow_exists(
        name=workflow.name, project_id=project_id, db=db
    )

    # Create new Workflow
    db_workflow = WorkflowV2(
        project_id=project_id,
        **workflow.dict(exclude_none=True, exclude={"task_list"})
    )
    debug(db_workflow)
    db.add(db_workflow)
    await db.commit()
    await db.refresh(db_workflow)

    task_user_list = await _get_user_tasks(user.id, db)
    debug(task_user_list)
    default_group_id = await _get_default_group_id(db)

    for wf_task in workflow.task_list:
        task_id = await _find_task_by_source_or_version(
            wf_task, user, db, task_user_list, default_group_id
        )
        new_wf_task = WorkflowTaskCreateV2(
            **wf_task.dict(exclude_none=True, exclude={"task"})
        )

        # Insert task into the workflow
        await _workflow_insert_task(
            **new_wf_task.dict(),
            workflow_id=db_workflow.id,
            task_id=task_id,
            db=db
        )

    await db.close()
    return db_workflow
