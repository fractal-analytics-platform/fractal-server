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
from ....models.v1 import Task
from ....models.v1 import WorkflowTask
from ....models.v2 import TaskV2
from ....schemas.v1 import TaskCreateV1
from ....schemas.v1 import TaskReadV1
from ....schemas.v1 import TaskUpdateV1
from ....security import current_active_user
from ....security import current_active_verified_user
from ....security import User
from ._aux_functions import _get_task_check_owner

router = APIRouter()

logger = set_logger(__name__)


@router.get("/", response_model=list[TaskReadV1])
async def get_list_task(
    user: User = Depends(current_active_user),
    args_schema: bool = True,
    db: AsyncSession = Depends(get_async_db),
) -> list[TaskReadV1]:
    """
    Get list of available tasks
    """
    stm = select(Task)
    res = await db.execute(stm)
    task_list = res.scalars().all()
    await db.close()
    if not args_schema:
        for task in task_list:
            setattr(task, "args_schema", None)

    return task_list


@router.get("/{task_id}/", response_model=TaskReadV1)
async def get_task(
    task_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> TaskReadV1:
    """
    Get info on a specific task
    """
    task = await db.get(Task, task_id)
    await db.close()
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Task not found"
        )
    return task


@router.patch("/{task_id}/", response_model=TaskReadV1)
async def patch_task(
    task_id: int,
    task_update: TaskUpdateV1,
    user: User = Depends(current_active_verified_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[TaskReadV1]:
    """
    Edit a specific task (restricted to superusers and task owner)
    """

    if task_update.source:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="patch_task endpoint cannot set `source`",
        )

    # Retrieve task from database
    db_task = await _get_task_check_owner(task_id=task_id, user=user, db=db)

    update = task_update.dict(exclude_unset=True)
    for key, value in update.items():
        if isinstance(value, str) or (
            key == "version" and value is None
        ):  # special case (issue 817)
            setattr(db_task, key, value)
        elif isinstance(value, dict):
            if key == "args_schema":
                setattr(db_task, key, value)
            else:
                current_dict = deepcopy(getattr(db_task, key))
                current_dict.update(value)
                setattr(db_task, key, current_dict)
        else:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Invalid {type(value)=} for {key=}",
            )

    await db.commit()
    await db.refresh(db_task)
    await db.close()
    return db_task


@router.post(
    "/", response_model=TaskReadV1, status_code=status.HTTP_201_CREATED
)
async def create_task(
    task: TaskCreateV1,
    user: User = Depends(current_active_verified_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[TaskReadV1]:
    """
    Create a new task
    """
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
    stm = select(Task).where(Task.source == task.source)
    res = await db.execute(stm)
    if res.scalars().all():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Source '{task.source}' already used by some TaskV1",
        )
    stm = select(TaskV2).where(TaskV2.source == task.source)
    res = await db.execute(stm)
    if res.scalars().all():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Source '{task.source}' already used by some TaskV2",
        )

    # Add task
    db_task = Task(**task.dict(), owner=owner)
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

    # Check that the Task is not in relationship with some WorkflowTask
    stm = select(WorkflowTask).filter(WorkflowTask.task_id == task_id)
    res = await db.execute(stm)
    workflowtask_list = res.scalars().all()
    if workflowtask_list:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Cannot remove Task {task_id} because it is currently "
                "imported in Workflows "
                f"{[x.workflow_id for x in workflowtask_list]}. "
                "If you want to remove this task, then you should first remove"
                " the workflows.",
            ),
        )

    await db.delete(db_task)
    await db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
