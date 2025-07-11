"""
Auxiliary functions to get task and task-group object from the database or
perform simple checks
"""
from typing import Any

from fastapi import HTTPException
from fastapi import status
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import UserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.app.routes.auth._aux_auth import _get_default_usergroup_id
from fractal_server.app.routes.auth._aux_auth import (
    _verify_user_belongs_to_group,
)
from fractal_server.app.schemas.v2 import TaskGroupActivityActionV2
from fractal_server.images.tools import merge_type_filters
from fractal_server.logger import set_logger

logger = set_logger(__name__)


async def _get_task_group_or_404(
    *, task_group_id: int, db: AsyncSession
) -> TaskGroupV2:
    """
    Get an existing task group or raise a 404.

    Arguments:
        task_group_id: The TaskGroupV2 id
        db: An asynchronous db session
    """
    task_group = await db.get(TaskGroupV2, task_group_id)
    if task_group is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"TaskGroupV2 {task_group_id} not found",
        )
    return task_group


async def _get_task_group_read_access(
    *,
    task_group_id: int,
    user_id: int,
    db: AsyncSession,
) -> TaskGroupV2:
    """
    Get a task group or raise a 403 if user has no read access.

    Arguments:
        task_group_id: ID of the required task group.
        user_id: ID of the current user.
        db: An asynchronous db session.
    """
    task_group = await _get_task_group_or_404(
        task_group_id=task_group_id, db=db
    )

    # Prepare exception to be used below
    forbidden_exception = HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=(
            "Current user has no read access to TaskGroupV2 "
            f"{task_group_id}.",
        ),
    )

    if task_group.user_id == user_id:
        return task_group
    elif task_group.user_group_id is None:
        raise forbidden_exception
    else:
        stm = (
            select(LinkUserGroup)
            .where(LinkUserGroup.group_id == task_group.user_group_id)
            .where(LinkUserGroup.user_id == user_id)
        )
        res = await db.execute(stm)
        link = res.scalar_one_or_none()
        if link is None:
            raise forbidden_exception
        else:
            return task_group


async def _get_task_group_full_access(
    *,
    task_group_id: int,
    user_id: int,
    db: AsyncSession,
) -> TaskGroupV2:
    """
    Get a task group or raise a 403 if user has no full access.

    Arguments:
        task_group_id: ID of the required task group.
        user_id: ID of the current user.
        db: An asynchronous db session
    """
    task_group = await _get_task_group_or_404(
        task_group_id=task_group_id, db=db
    )

    if task_group.user_id == user_id:
        return task_group
    else:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                "Current user has no full access to "
                f"TaskGroupV2 {task_group_id}.",
            ),
        )


async def _get_task_or_404(*, task_id: int, db: AsyncSession) -> TaskV2:
    """
    Get an existing task or raise a 404.

    Arguments:
        task_id: ID of the required task.
        db: An asynchronous db session
    """
    task = await db.get(TaskV2, task_id)
    if task is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"TaskV2 {task_id} not found",
        )
    return task


async def _get_task_full_access(
    *,
    task_id: int,
    user_id: int,
    db: AsyncSession,
) -> TaskV2:
    """
    Get an existing task or raise a 404.

    Arguments:
        task_id: ID of the required task.
        user_id: ID of the current user.
        db: An asynchronous db session.
    """
    task = await _get_task_or_404(task_id=task_id, db=db)
    await _get_task_group_full_access(
        task_group_id=task.taskgroupv2_id, user_id=user_id, db=db
    )
    return task


async def _get_task_read_access(
    *,
    task_id: int,
    user_id: int,
    db: AsyncSession,
    require_active: bool = False,
) -> TaskV2:
    """
    Get an existing task or raise a 404.

    Arguments:
        task_id: ID of the required task.
        user_id: ID of the current user.
        db: An asynchronous db session.
        require_active: If set, fail when the task group is not `active`
    """
    task = await _get_task_or_404(task_id=task_id, db=db)
    task_group = await _get_task_group_read_access(
        task_group_id=task.taskgroupv2_id, user_id=user_id, db=db
    )
    if require_active:
        if not task_group.active:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Error: task {task_id} ({task.name}) is not active.",
            )
    return task


async def _get_valid_user_group_id(
    *,
    user_group_id: int | None = None,
    private: bool,
    user_id: int,
    db: AsyncSession,
) -> int | None:
    """
    Validate query parameters for endpoints that create some task(s).

    Arguments:
        user_group_id:
        private:
        user_id: ID of the current user
        db: An asynchronous db session.
    """
    if (user_group_id is not None) and (private is True):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Cannot set both {user_group_id=} and {private=}",
        )
    elif private is True:
        user_group_id = None
    elif user_group_id is None:
        user_group_id = await _get_default_usergroup_id(db=db)
    else:
        await _verify_user_belongs_to_group(
            user_id=user_id, user_group_id=user_group_id, db=db
        )
    return user_group_id


async def _get_collection_task_group_activity_status_message(
    task_group_id: int,
    db: AsyncSession,
) -> str:
    res = await db.execute(
        select(TaskGroupActivityV2)
        .where(TaskGroupActivityV2.taskgroupv2_id == task_group_id)
        .where(TaskGroupActivityV2.action == TaskGroupActivityActionV2.COLLECT)
    )
    task_group_activity_list = res.scalars().all()
    if len(task_group_activity_list) > 1:
        msg = (
            "\nWarning: "
            "Expected only one TaskGroupActivityV2 associated to TaskGroup "
            f"{task_group_id}, found {len(task_group_activity_list)} "
            f"(IDs: {[tga.id for tga in task_group_activity_list]})."
            "Warning: this should have not happened, please contact an admin."
        )
    elif len(task_group_activity_list) == 1:
        msg = (
            "\nNote:"
            "There exists another task-group collection "
            f"(activity ID={task_group_activity_list[0].id}) for "
            f"this task group (ID={task_group_id}), with status "
            f"'{task_group_activity_list[0].status}'."
        )
    else:
        msg = ""
    return msg


async def _verify_non_duplication_user_constraint(
    db: AsyncSession,
    user_id: int,
    pkg_name: str,
    version: str | None,
):
    stm = (
        select(TaskGroupV2)
        .where(TaskGroupV2.user_id == user_id)
        .where(TaskGroupV2.pkg_name == pkg_name)
        .where(TaskGroupV2.version == version)
    )
    res = await db.execute(stm)
    duplicate = res.scalars().all()
    if duplicate:
        user = await db.get(UserOAuth, user_id)
        if len(duplicate) > 1:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    "Invalid state:\n"
                    f"User '{user.email}' already owns {len(duplicate)} task "
                    f"groups with name='{pkg_name}' and {version=} "
                    f"(IDs: {[group.id for group in duplicate]}).\n"
                    "This should have not happened: please contact an admin."
                ),
            )
        state_msg = await _get_collection_task_group_activity_status_message(
            duplicate[0].id, db
        )
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"User '{user.email}' already owns a task group "
                f"with name='{pkg_name}' and {version=}.{state_msg}"
            ),
        )


async def _verify_non_duplication_group_constraint(
    db: AsyncSession,
    user_group_id: int | None,
    pkg_name: str,
    version: str | None,
):
    if user_group_id is None:
        return

    stm = (
        select(TaskGroupV2)
        .where(TaskGroupV2.user_group_id == user_group_id)
        .where(TaskGroupV2.pkg_name == pkg_name)
        .where(TaskGroupV2.version == version)
    )
    res = await db.execute(stm)
    duplicate = res.scalars().all()
    if duplicate:
        user_group = await db.get(UserGroup, user_group_id)
        if len(duplicate) > 1:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    "Invalid state:\n"
                    f"UserGroup '{user_group.name}' already owns "
                    f"{len(duplicate)} task groups with name='{pkg_name}' and "
                    f"{version=} (IDs: {[group.id for group in duplicate]}).\n"
                    "This should have not happened: please contact an admin."
                ),
            )
        state_msg = await _get_collection_task_group_activity_status_message(
            duplicate[0].id, db
        )
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"UserGroup {user_group.name} already owns a task group "
                f"with {pkg_name=} and {version=}.{state_msg}"
            ),
        )


async def _verify_non_duplication_group_path(
    path: str | None,
    db: AsyncSession,
) -> None:
    """
    Verify uniqueness of non-`None` `TaskGroupV2.path`
    """
    if path is None:
        return
    stm = select(TaskGroupV2.id).where(TaskGroupV2.path == path)
    res = await db.execute(stm)
    duplicate_ids = res.scalars().all()
    if duplicate_ids:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Other TaskGroups already have {path=}: "
                f"{sorted(duplicate_ids)}."
            ),
        )


async def _add_warnings_to_workflow_tasks(
    wftask_list: list[WorkflowTaskV2], user_id: int, db: AsyncSession
) -> list[dict[str, Any]]:
    wftask_list_with_warnings = []
    for wftask in wftask_list:
        wftask_data = dict(wftask.model_dump(), task=wftask.task)
        try:
            task_group = await _get_task_group_read_access(
                task_group_id=wftask.task.taskgroupv2_id,
                user_id=user_id,
                db=db,
            )
            if not task_group.active:
                wftask_data["warning"] = "Task is not active."
        except HTTPException:
            wftask_data["warning"] = "Current user has no access to this task."
        wftask_list_with_warnings.append(wftask_data)
    return wftask_list_with_warnings


def _check_type_filters_compatibility(
    *,
    task_input_types: dict[str, bool],
    wftask_type_filters: dict[str, bool],
) -> None:
    """
    Wrap `merge_type_filters` and raise `HTTPException` if needed.
    """
    try:
        merge_type_filters(
            task_input_types=task_input_types,
            wftask_type_filters=wftask_type_filters,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Incompatible type filters.\nOriginal error: {str(e)}",
        )
