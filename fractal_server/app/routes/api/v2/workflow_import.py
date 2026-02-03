from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status
from pydantic import BaseModel
from sqlmodel import or_
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.models.v2 import WorkflowV2
from fractal_server.app.routes.api.v2._aux_task_group_disambiguation import (
    _disambiguate_task_groups,
)
from fractal_server.app.routes.auth import get_api_user
from fractal_server.app.routes.auth._aux_auth import (
    _get_default_usergroup_id_or_none,
)
from fractal_server.app.routes.aux._versions import _version_sort_key
from fractal_server.app.schemas.v2 import TaskImport
from fractal_server.app.schemas.v2 import WorkflowImport
from fractal_server.app.schemas.v2 import WorkflowTaskCreate
from fractal_server.app.schemas.v2.sharing import ProjectPermissions
from fractal_server.exceptions import HTTPExceptionWithData
from fractal_server.logger import set_logger

from ._aux_functions import _check_workflow_exists
from ._aux_functions import _get_project_check_access
from ._aux_functions import _get_user_resource_id
from ._aux_functions import _workflow_insert_task
from ._aux_functions_tasks import _add_warnings_to_workflow_tasks
from ._aux_functions_tasks import _check_type_filters_compatibility

router = APIRouter()


logger = set_logger(__name__)


class TaskAvailable(BaseModel):
    task_id: int
    taskgroup_id: int
    version: str | None
    active: bool


async def _get_user_accessible_taskgroups(
    *,
    user_id: int,
    user_resource_id: int,
    db: AsyncSession,
) -> list[TaskGroupV2]:
    """
    Retrieve list of task groups that the user has access to.
    """

    stm = (
        select(TaskGroupV2)
        .where(
            or_(
                TaskGroupV2.user_id == user_id,
                TaskGroupV2.user_group_id.in_(
                    select(LinkUserGroup.group_id).where(
                        LinkUserGroup.user_id == user_id
                    )
                ),
            )
        )
        .where(TaskGroupV2.resource_id == user_resource_id)
    )
    res = await db.execute(stm)
    accessible_task_groups = res.scalars().all()
    logger.debug(
        f"Found {len(accessible_task_groups)} accessible "
        f"task groups for {user_id=}."
    )
    return accessible_task_groups


async def _get_task_by_taskimport(
    *,
    task_import: TaskImport,
    task_groups_list: list[TaskGroupV2],
    user_id: int,
    default_group_id: int | None,
    db: AsyncSession,
) -> int | list[dict[str, str | int]]:
    """
    Find a task based on `task_import`.

    Args:
        task_import: Info on task to be imported.
        task_groups_list: Current list of valid task groups.
        user_id: ID of current user.
        default_group_id: ID of default user group.
        db: Asynchronous database session.

    Return:
        `id` of the matching task, or a list of available tasks.
    """

    logger.debug(f"[_get_task_by_taskimport] START, {task_import=}")

    # Filter by `pkg_name` and by presence of a task with given `name`.
    matching_task_groups = [
        task_group
        for task_group in task_groups_list
        if (
            task_group.pkg_name == task_import.pkg_name
            and task_import.name in [task.name for task in task_group.task_list]
        )
    ]
    if len(matching_task_groups) < 1:
        logger.debug(
            "[_get_task_by_taskimport] "
            f"No task group with {task_import.pkg_name=} "
            f"and a task with {task_import.name=}."
        )
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"Missing match for {task_import.name=} {task_import.pkg_name=}"
            ),
        )

    # Determine target `version`
    if task_import.version is None:
        logger.debug(
            "[_get_task_by_taskimport] "
            "No version requested, looking for latest."
        )
        target_version = max(
            [tg.version for tg in matching_task_groups], key=_version_sort_key
        )
        logger.debug(
            f"[_get_task_by_taskimport] Latest version set to {target_version}."
        )
    else:
        target_version = task_import.version

    # Filter task groups by version
    final_matching_task_groups = list(
        filter(lambda tg: tg.version == target_version, matching_task_groups)
    )

    if len(final_matching_task_groups) < 1:
        logger.debug(
            "[_get_task_by_taskimport] "
            "No task group left after filtering by version."
        )
        return [
            TaskAvailable(
                task_id=next(task.id for task in tg.task_list),
                taskgroup_id=tg.id,
                version=tg.version,
                active=tg.active,
            ).model_dump()
            for tg in matching_task_groups
        ]
    elif len(final_matching_task_groups) == 1:
        final_task_group = final_matching_task_groups[0]
        logger.debug(
            "[_get_task_by_taskimport] "
            "Found a single task group, after filtering by version."
        )
    else:
        logger.debug(
            "[_get_task_by_taskimport] "
            f"Found {len(final_matching_task_groups)} task groups, "
            "after filtering by version."
        )
        final_task_group = await _disambiguate_task_groups(
            matching_task_groups=final_matching_task_groups,
            user_id=user_id,
            db=db,
            default_group_id=default_group_id,
        )
        if final_task_group is None:
            logger.debug(
                "[_get_task_by_taskimport] Disambiguation returned None."
            )
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail=(
                    "Disambiguation returned None for requested task "
                    f"{task_import}."
                ),
            )

    # Find task with given name
    task_id = next(
        iter(
            task.id
            for task in final_task_group.task_list
            if task.name == task_import.name
        ),
        None,
    )
    if task_id is None:
        logger.error(
            "[_get_task_by_taskimport] UnreachableBranchError:"
            "likely be due to a race condition on TaskGroups."
        )
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT)

    logger.debug(f"[_get_task_by_taskimport] END, {task_import=}, {task_id=}.")

    return task_id


@router.post(
    "/project/{project_id}/workflow/import/",
    status_code=status.HTTP_201_CREATED,
)
async def import_workflow(
    project_id: int,
    workflow_import: WorkflowImport,
    user: UserOAuth = Depends(get_api_user),
    db: AsyncSession = Depends(get_async_db),
    flexible_version: bool = False,
):
    """
    Import an existing workflow into a project and create required objects.
    """

    user_resource_id = await _get_user_resource_id(user_id=user.id, db=db)

    # Preliminary checks
    await _get_project_check_access(
        project_id=project_id,
        user_id=user.id,
        required_permissions=ProjectPermissions.WRITE,
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
        user_resource_id=user_resource_id,
    )
    default_group_id = await _get_default_usergroup_id_or_none(db)

    list_wf_tasks = []
    list_task_ids = [
        await _get_task_by_taskimport(
            task_import=wf_task.task,
            user_id=user.id,
            default_group_id=default_group_id,
            task_groups_list=task_group_list,
            db=db,
        )
        for wf_task in workflow_import.task_list
    ]

    if any(not isinstance(item, int) for item in list_task_ids):
        raise HTTPExceptionWithData(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            data=[
                {
                    "outcome": "success",
                    "pkg_name": wf_task.task.pkg_name,
                    "version": wf_task.task.version,
                    "task_name": wf_task.task.name,
                    "task_id": task_id_or_available_tasks,
                }
                if isinstance(task_id_or_available_tasks, int)
                else {
                    "outcome": "fail",
                    "pkg_name": wf_task.task.pkg_name,
                    "version": wf_task.task.version,
                    "task_name": wf_task.task.name,
                    "available_tasks": task_id_or_available_tasks,
                }
                for wf_task, task_id_or_available_tasks in zip(
                    workflow_import.task_list, list_task_ids
                )
            ],
        )

    for wf_task, task_id in zip(workflow_import.task_list, list_task_ids):
        new_wf_task = WorkflowTaskCreate(
            **wf_task.model_dump(exclude_none=True, exclude={"task"})
        )
        list_wf_tasks.append(new_wf_task)
        task = await db.get(TaskV2, task_id)
        _check_type_filters_compatibility(
            task_input_types=task.input_types,
            wftask_type_filters=new_wf_task.type_filters,
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
