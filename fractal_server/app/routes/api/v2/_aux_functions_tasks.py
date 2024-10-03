"""
Auxiliary functions to get task and task-group object from the database or
perform simple checks
"""
from typing import Optional

from fastapi import HTTPException
from fastapi import status
from sqlmodel import select

from ....db import AsyncSession
from ....models import LinkUserGroup
from ....models.v2 import TaskGroupV2
from ....models.v2 import TaskV2
from ...auth._aux_auth import _get_default_user_group_id
from ...auth._aux_auth import _verify_user_belongs_to_group


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
                detail="Cannot insert non-active tasks into a workflow.",
            )
    return task


async def _get_valid_user_group_id(
    *,
    user_group_id: Optional[int] = None,
    private: bool,
    user_id: int,
    db: AsyncSession,
) -> Optional[int]:
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
        user_group_id = await _get_default_user_group_id(db=db)
    else:
        await _verify_user_belongs_to_group(
            user_id=user_id, user_group_id=user_group_id, db=db
        )
    return user_group_id
