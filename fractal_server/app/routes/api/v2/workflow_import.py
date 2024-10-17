from typing import Optional
from typing import Union

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
from fractal_server.app.models.v2.task import TaskGroupV2
from fractal_server.app.routes.auth import current_active_user
from fractal_server.app.routes.auth._aux_auth import _get_default_usergroup_id
from fractal_server.app.schemas.v2.task import TaskImportV2

router = APIRouter()


async def _get_user_accessible_taskgroups(
    user_id: int, db: AsyncSession
) -> list[TaskGroupV2]:
    """Retrieve tasks that belong to the user."""
    stm = select(TaskGroupV2).where(
        or_(
            TaskGroupV2.user_id == user_id,
            TaskGroupV2.user_group_id.in_(
                select(LinkUserGroup.group_id).where(
                    LinkUserGroup.user_id == user_id
                )
            ),
        )
    )
    res = await db.execute(stm)
    accessible_task_groups = res.scalars().all()
    return accessible_task_groups


async def _find_task_by_source_or_version(
    task_import: Union[TaskImportV2, TaskImportV2Legacy],
    user_id: int,
    db: AsyncSession,
    task_groups_list: list[TaskGroupV2],
    default_group_id: int,
) -> int:
    """Find a task by source or version."""
    if isinstance(task_import, TaskImportV2Legacy):
        task_id = await _get_task_by_source(
            task_import.source, task_groups_list
        )
    else:
        task_id = await _get_task_by_version(
            task_import, user_id, db, task_groups_list, default_group_id
        )
    if not task_id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Expected one TaskV2 to match the imported one.",
        )

    return task_id


async def _get_task_by_source(source: str, task_groups_list) -> Optional[int]:
    """Find a task by its source."""

    for task_group in task_groups_list:
        for task in task_group.task_list:
            if task.source == source:
                return task.id
    return None


async def _get_task_by_version(
    task_import, user_id, db, task_groups_list, default_group_id
) -> Optional[int]:
    """Find a task by version."""

    matching_task_groups = [
        task_group
        for task_group in task_groups_list
        if (
            task_group.pkg_name == task_import.pkg_name
            and task_import.name
            in [task.name for task in task_group.task_list]
        )
    ]
    if len(matching_task_groups) < 1:
        return None

    if task_import.version is None:
        latest_task = max(
            matching_task_groups, key=lambda tg: tg.version or ""
        )
        version = latest_task.version
        if version == "":
            version = None
    else:
        version = task_import.version

    final_matching_task_groups = list(
        filter(lambda tg: tg.version == version, task_groups_list)
    )

    if len(final_matching_task_groups) < 1:
        return None

    elif len(final_matching_task_groups) > 1:
        final_task_group = await _disambiguate_task_groups(
            matching_task_groups, user_id, db, default_group_id
        )

    else:
        final_task_group = final_matching_task_groups[0]

    task_id = next(
        task.id
        for task in final_task_group.task_list
        if task.name == task_import.name
    )
    # return if a list is empty
    return task_id


async def _disambiguate_task_groups(
    matching_task_groups, user_id, db, default_group_id
) -> Optional[TaskV2]:
    """Resolve conflicts when multiple tasks match."""
    for task_group in matching_task_groups:
        if task_group.user_id == user_id:
            return task_group
    for task_group in matching_task_groups:
        if task_group.user_group_id == default_group_id:
            return task_group

    stm = (
        select(LinkUserGroup.group_id)
        .where(
            LinkUserGroup.group_id.in_(
                [
                    task_group.user_group_id
                    for task_group in matching_task_groups
                ]
            )
        )
        .order_by(LinkUserGroup.timestamp_created.asc().limit(1))
    )
    res = await db.execute(stm)
    oldest_user_group_id = res.scalars().one()

    for task_group in matching_task_groups:
        if task_group.user_group_id == oldest_user_group_id:
            return task_group
    return None


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

    task_group_list = await _get_user_accessible_taskgroups(user.id, db)
    default_group_id = await _get_default_usergroup_id(db)

    list_wf_tasks = []
    for wf_task in workflow.task_list:
        task_id = await _find_task_by_source_or_version(
            wf_task.task, user.id, db, task_group_list, default_group_id
        )
        new_wf_task = WorkflowTaskCreateV2(
            **wf_task.dict(exclude_none=True, exclude={"task"})
        )
        list_wf_tasks.append(new_wf_task)

    # Create new Workflow
    db_workflow = WorkflowV2(
        project_id=project_id,
        **workflow.dict(exclude_none=True, exclude={"task_list"})
    )
    db.add(db_workflow)
    await db.commit()
    await db.refresh(db_workflow)

    # Insert task into the workflow
    for new_wf_task in list_wf_tasks:
        await _workflow_insert_task(
            **new_wf_task.dict(),
            workflow_id=db_workflow.id,
            task_id=task_id,
            db=db
        )

    return db_workflow
