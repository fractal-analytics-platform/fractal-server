from typing import Optional

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
from ....schemas.v2 import WorkflowReadV2WithWarnings
from ....schemas.v2 import WorkflowTaskCreateV2
from ._aux_functions import _check_workflow_exists
from ._aux_functions import _get_project_check_owner
from ._aux_functions import _workflow_insert_task
from ._aux_functions_tasks import _add_warnings_to_workflow_tasks
from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2.task import TaskGroupV2
from fractal_server.app.routes.auth import current_active_user
from fractal_server.app.routes.auth._aux_auth import _get_default_usergroup_id
from fractal_server.app.schemas.v2.task import TaskImportV2

router = APIRouter()


async def _get_user_accessible_taskgroups(
    *,
    user_id: int,
    db: AsyncSession,
) -> list[TaskGroupV2]:

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
    task_groups = res.scalars().all()
    return task_groups


async def _get_task_by_source(
    source: str,
    task_groups_list: list[TaskGroupV2],
    db: AsyncSession,
) -> Optional[int]:

    stm = select(TaskV2.id).where(
        TaskV2.source == source,
        TaskGroupV2.id.in_([tg.id for tg in task_groups_list]),
    )
    res = await db.execute(stm)
    task_id = res.scalar()
    return task_id


async def _disambiguate_task_groups(
    *,
    matching_task_group_ids: list[int],
    user_id: int,
    db: AsyncSession,
    default_group_id: int,
) -> Optional[int]:

    # Highest priority: task groups created by the user
    stm = select(TaskGroupV2.id).where(
        TaskGroupV2.id.in_(matching_task_group_ids),
        TaskGroupV2.user_id == user_id,
    )
    res = await db.execute(stm)
    task_group_id = res.scalar()
    if task_group_id:
        return task_group_id

    # Medium priority: task groups owned by the default user group
    stm = select(TaskGroupV2.id).where(
        TaskGroupV2.id.in_(matching_task_group_ids),
        TaskGroupV2.user_group_id == default_group_id,
    )
    res = await db.execute(stm)
    task_group_id = res.scalar()
    if task_group_id:
        return task_group_id

    # Lowest priority: task groups owned by other groups,
    # sorted by the age of the user/user group link
    stm = (
        select(LinkUserGroup.group_id)
        .where(LinkUserGroup.user_id == user_id)
        .where(LinkUserGroup.group_id.in_(matching_task_group_ids))
        .order_by(LinkUserGroup.timestamp_created.asc())
    )
    res = await db.execute(stm)
    oldest_user_group_id = res.scalar()
    if oldest_user_group_id:
        stm = select(TaskGroupV2.id).where(
            TaskGroupV2.user_group_id == oldest_user_group_id,
            TaskGroupV2.id.in_(matching_task_group_ids),
        )
        res = await db.execute(stm)
        return res.scalar()

    return None


async def _get_task_by_taskimport(
    *,
    task_import: TaskImportV2,
    user_id: int,
    default_group_id: int,
    task_groups_list: list[int],
    db: AsyncSession,
) -> Optional[int]:

    # Filter by `pkg_name` and task name
    stm = (
        select(TaskGroupV2.id)
        .where(
            TaskGroupV2.pkg_name == task_import.pkg_name,
            TaskGroupV2.id.in_([tg.id for tg in task_groups_list]),
            TaskV2.name == task_import.name,
        )
        .join(TaskV2, TaskV2.taskgroupv2_id == TaskGroupV2.id)
    )
    res = await db.execute(stm)
    matching_task_group_ids = [row[0] for row in res.fetchall()]

    if not matching_task_group_ids:
        return None

    # Determine target version
    version = task_import.version
    if version is None:
        stm = (
            select(TaskGroupV2.version)
            .where(TaskGroupV2.id.in_(matching_task_group_ids))
            .order_by(TaskGroupV2.version.desc())
        )
        res = await db.execute(stm)
        version = res.scalars().first()

    # Filter task groups by version
    stm = select(TaskGroupV2.id).where(
        TaskGroupV2.id.in_(matching_task_group_ids),
        TaskGroupV2.version == version,
    )
    res = await db.execute(stm)
    final_matching_task_group_ids = [row[0] for row in res.fetchall()]

    if len(final_matching_task_group_ids) == 1:
        final_task_group_id = final_matching_task_group_ids[0]
    else:
        final_task_group_id = await _disambiguate_task_groups(
            matching_task_group_ids=final_matching_task_group_ids,
            user_id=user_id,
            db=db,
            default_group_id=default_group_id,
        )
        if final_task_group_id is None:
            return None

    # Find the task with the given name in the resolved task group
    stm = select(TaskV2.id).where(
        TaskV2.task_group_id == final_task_group_id,
        TaskV2.name == task_import.name,
    )
    res = await db.execute(stm)
    return res.scalar()


@router.post(
    "/project/{project_id}/workflow/import/",
    response_model=WorkflowReadV2WithWarnings,
    status_code=status.HTTP_201_CREATED,
)
async def import_workflow(
    project_id: int,
    workflow_import: WorkflowImportV2,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> WorkflowReadV2WithWarnings:
    """
    Import an existing workflow into a project and create required objects.
    """

    # Preliminary checks
    await _get_project_check_owner(
        project_id=project_id,
        user_id=user.id,
        db=db,
    )
    await _check_workflow_exists(
        name=workflow_import.name,
        project_id=project_id,
        db=db,
    )

    task_group_list = await _get_user_accessible_taskgroups(
        user_id=user.id,
        db=db,
    )
    default_group_id = await _get_default_usergroup_id(db)

    list_wf_tasks = []
    for wf_task in workflow_import.task_list:
        task_import = wf_task.task
        if isinstance(task_import, TaskImportV2Legacy):
            task_id = await _get_task_by_source(
                source=task_import.source,
                task_groups_list=task_group_list,
            )
        else:
            task_id = await _get_task_by_taskimport(
                task_import=task_import,
                user_id=user.id,
                default_group_id=default_group_id,
                task_groups_list=task_group_list,
                db=db,
            )
        if task_id is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Could not find a task matching with {wf_task.task}.",
            )
        new_wf_task = WorkflowTaskCreateV2(
            **wf_task.dict(exclude_none=True, exclude={"task"})
        )
        list_wf_tasks.append(new_wf_task)

    # Create new Workflow
    db_workflow = WorkflowV2(
        project_id=project_id,
        **workflow_import.dict(exclude_none=True, exclude={"task_list"}),
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
            db=db,
        )

    # Add warnings for non-active tasks (or non-accessible tasks,
    # although that should never happen)
    wftask_list_with_warnings = await _add_warnings_to_workflow_tasks(
        wftask_list=db_workflow.task_list, user_id=user.id, db=db
    )
    workflow_data = dict(
        **db_workflow.model_dump(),
        project=db_workflow.project,
        task_list=wftask_list_with_warnings,
    )

    return workflow_data
