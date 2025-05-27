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
from ._aux_functions_tasks import _check_type_filters_compatibility
from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.routes.api.v2._aux_task_group_disambiguation import (
    _disambiguate_task_groups,
)
from fractal_server.app.routes.auth import current_active_user
from fractal_server.app.routes.auth._aux_auth import _get_default_usergroup_id
from fractal_server.app.schemas.v2 import TaskImportV2
from fractal_server.logger import set_logger

router = APIRouter()


logger = set_logger(__name__)


async def _get_user_accessible_taskgroups(
    *,
    user_id: int,
    db: AsyncSession,
) -> list[TaskGroupV2]:
    """
    Retrieve list of task groups that the user has access to.
    """
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
    logger.info(
        f"Found {len(accessible_task_groups)} accessible "
        f"task groups for {user_id=}."
    )
    return accessible_task_groups


async def _get_task_by_source(
    source: str,
    task_groups_list: list[TaskGroupV2],
) -> int | None:
    """
    Find task with a given source.

    Args:
        source: `source` of the task to be imported.
        task_groups_list: Current list of valid task groups.

    Return:
        `id` of the matching task, or `None`.
    """
    task_id = next(
        iter(
            task.id
            for task_group in task_groups_list
            for task in task_group.task_list
            if task.source == source
        ),
        None,
    )
    return task_id


async def _get_task_by_taskimport(
    *,
    task_import: TaskImportV2,
    task_groups_list: list[TaskGroupV2],
    user_id: int,
    default_group_id: int,
    db: AsyncSession,
) -> int | None:
    """
    Find a task based on `task_import`.

    Args:
        task_import: Info on task to be imported.
        task_groups_list: Current list of valid task groups.
        user_id: ID of current user.
        default_group_id: ID of default user group.
        db: Asynchronous database session.

    Return:
        `id` of the matching task, or `None`.
    """

    logger.info(f"[_get_task_by_taskimport] START, {task_import=}")

    # Filter by `pkg_name` and by presence of a task with given `name`.
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
        logger.info(
            "[_get_task_by_taskimport] "
            f"No task group with {task_import.pkg_name=} "
            f"and a task with {task_import.name=}."
        )
        return None

    # Determine target `version`
    # Note that task_import.version cannot be "", due to a validator
    if task_import.version is None:
        logger.info(
            "[_get_task_by_taskimport] "
            "No version requested, looking for latest."
        )
        latest_task = max(
            matching_task_groups, key=lambda tg: tg.version or ""
        )
        version = latest_task.version
        logger.info(
            f"[_get_task_by_taskimport] Latest version set to {version}."
        )
    else:
        version = task_import.version

    # Filter task groups by version
    final_matching_task_groups = list(
        filter(lambda tg: tg.version == version, task_groups_list)
    )

    if len(final_matching_task_groups) < 1:
        logger.info(
            "[_get_task_by_taskimport] "
            "No task group left after filtering by version."
        )
        return None
    elif len(final_matching_task_groups) == 1:
        final_task_group = final_matching_task_groups[0]
        logger.info(
            "[_get_task_by_taskimport] "
            "Found a single task group, after filtering by version."
        )
    else:
        logger.info(
            "[_get_task_by_taskimport] "
            "Found many task groups, after filtering by version."
        )
        final_task_group = await _disambiguate_task_groups(
            matching_task_groups=matching_task_groups,
            user_id=user_id,
            db=db,
            default_group_id=default_group_id,
        )
        if final_task_group is None:
            logger.info(
                "[_get_task_by_taskimport] Disambiguation returned None."
            )
            return None

    # Find task with given name
    task_id = next(
        iter(
            task.id
            for task in final_task_group.task_list
            if task.name == task_import.name
        ),
        None,
    )

    logger.info(f"[_get_task_by_taskimport] END, {task_import=}, {task_id=}.")

    return task_id


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
    list_task_ids = []
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
            **wf_task.model_dump(exclude_none=True, exclude={"task"})
        )
        list_wf_tasks.append(new_wf_task)
        list_task_ids.append(task_id)

    for wftask, task_id in zip(list_wf_tasks, list_task_ids):
        task = await db.get(TaskV2, task_id)
        _check_type_filters_compatibility(
            task_input_types=task.input_types,
            wftask_type_filters=wftask.type_filters,
        )

    # Create new Workflow
    db_workflow = WorkflowV2(
        project_id=project_id,
        **workflow_import.model_dump(exclude_none=True, exclude={"task_list"}),
    )
    db.add(db_workflow)
    await db.commit()
    await db.refresh(db_workflow)

    # Insert task into the workflow
    for ind, new_wf_task in enumerate(list_wf_tasks):
        await _workflow_insert_task(
            **new_wf_task.model_dump(),
            workflow_id=db_workflow.id,
            task_id=list_task_ids[ind],
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
