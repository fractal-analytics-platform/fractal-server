from copy import deepcopy  # noqa
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from sqlmodel import select

from .....logger import set_logger
from ....db import AsyncSession
from ....db import get_async_db
from ....models.v1 import Task as TaskV1
from ....models.v2 import TaskV2
from ....models.v2 import WorkflowTaskV2
from ....schemas.v2 import TaskCreateV2
from ....schemas.v2 import TaskReadV2
from ....schemas.v2 import TaskUpdateV2
from ....security import current_active_user
from ....security import current_active_verified_user
from ....security import User
from ._aux_functions import _get_task_check_owner

router = APIRouter()

logger = set_logger(__name__)


@router.get("/", response_model=list[TaskReadV2])
async def get_list_task(
    args_schema_parallel: bool = True,
    args_schema_non_parallel: bool = True,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[TaskReadV2]:
    """
    Get list of available tasks
    """
    stm = select(TaskV2)
    res = await db.execute(stm)
    task_list = res.scalars().all()
    await db.close()
    if args_schema_parallel is False:
        for task in task_list:
            setattr(task, "args_schema_parallel", None)
    if args_schema_non_parallel is False:
        for task in task_list:
            setattr(task, "args_schema_non_parallel", None)

    return task_list


@router.get("/{task_id}/", response_model=TaskReadV2)
async def get_task(
    task_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> TaskReadV2:
    """
    Get info on a specific task
    """
    task = await db.get(TaskV2, task_id)
    await db.close()
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="TaskV2 not found"
        )
    return task


@router.patch("/{task_id}/", response_model=TaskReadV2)
async def patch_task(
    task_id: int,
    task_update: TaskUpdateV2,
    user: User = Depends(current_active_verified_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[TaskReadV2]:
    """
    Edit a specific task (restricted to superusers and task owner)
    """

    # Retrieve task from database
    db_task = await _get_task_check_owner(task_id=task_id, user=user, db=db)
    update = task_update.dict(exclude_unset=True)

    # Forbid changes that set a previously unset command
    if db_task.type == "non_parallel" and "command_parallel" in update:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Cannot set an unset `command_parallel`.",
        )
    if db_task.type == "parallel" and "command_non_parallel" in update:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Cannot set an unset `command_non_parallel`.",
        )

    for key, value in update.items():
        setattr(db_task, key, value)

    await db.commit()
    await db.refresh(db_task)
    await db.close()
    return db_task


@router.post(
    "/", response_model=TaskReadV2, status_code=status.HTTP_201_CREATED
)
async def create_task(
    task: TaskCreateV2,
    user: User = Depends(current_active_verified_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[TaskReadV2]:
    """
    Create a new task
    """

    if task.command_non_parallel is None:
        task_type = "parallel"
    elif task.command_parallel is None:
        task_type = "non_parallel"
    else:
        task_type = "compound"

    if task_type == "parallel" and (
        task.args_schema_non_parallel is not None
        or task.meta_non_parallel is not None
    ):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "Cannot set `TaskV2.args_schema_non_parallel` or "
                "`TaskV2.args_schema_non_parallel` if TaskV2 is parallel"
            ),
        )
    elif task_type == "non_parallel" and (
        task.args_schema_parallel is not None or task.meta_parallel is not None
    ):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "Cannot set `TaskV2.args_schema_parallel` or "
                "`TaskV2.args_schema_parallel` if TaskV2 is non_parallel"
            ),
        )

    # Set task.owner attribute
    if user.username:
        owner = user.username
    elif user.slurm_user:
        owner = user.slurm_user
    else:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "Cannot add a new task because current user does not "
                "have `username` or `slurm_user` attributes."
            ),
        )

    # Prepend owner to task.source
    task.source = f"{owner}:{task.source}"

    # Verify that source is not already in use (note: this check is only useful
    # to provide a user-friendly error message, but `task.source` uniqueness is
    # already guaranteed by a constraint in the table definition).
    stm = select(TaskV2).where(TaskV2.source == task.source)
    res = await db.execute(stm)
    if res.scalars().all():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Source '{task.source}' already used by some TaskV2",
        )
    stm = select(TaskV1).where(TaskV1.source == task.source)
    res = await db.execute(stm)
    if res.scalars().all():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Source '{task.source}' already used by some TaskV1",
        )
    # Add task
    db_task = TaskV2(**task.dict(), owner=owner, type=task_type)
    db.add(db_task)
    await db.commit()
    await db.refresh(db_task)
    await db.close()
    return db_task


@router.delete("/{task_id}/", status_code=204)
async def delete_task(
    task_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Response:
    """
    Delete a task
    """

    db_task = await _get_task_check_owner(task_id=task_id, user=user, db=db)

    # Check that the TaskV2 is not in relationship with some WorkflowTaskV2
    stm = select(WorkflowTaskV2).filter(WorkflowTaskV2.task_id == task_id)
    res = await db.execute(stm)
    workflowtask_list = res.scalars().all()
    if workflowtask_list:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Cannot remove TaskV2 {task_id} because it is currently "
                "imported in WorkflowsV2 "
                f"{[x.workflow_id for x in workflowtask_list]}. "
                "If you want to remove this task, then you should first remove"
                " the workflows.",
            ),
        )

    await db.delete(db_task)
    await db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
