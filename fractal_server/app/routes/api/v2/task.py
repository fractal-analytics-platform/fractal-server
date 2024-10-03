from copy import deepcopy  # noqa
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from sqlmodel import or_
from sqlmodel import select

from ...aux.validate_user_settings import verify_user_has_settings
from ._aux_functions_tasks import _get_task_full_access
from ._aux_functions_tasks import _get_task_read_access
from ._aux_functions_tasks import _get_valid_user_group_id
from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v1 import Task as TaskV1
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.routes.auth import current_active_user
from fractal_server.app.routes.auth import current_active_verified_user
from fractal_server.app.schemas.v2 import TaskCreateV2
from fractal_server.app.schemas.v2 import TaskReadV2
from fractal_server.app.schemas.v2 import TaskUpdateV2
from fractal_server.logger import set_logger

router = APIRouter()

logger = set_logger(__name__)


@router.get("/", response_model=list[TaskReadV2])
async def get_list_task(
    args_schema_parallel: bool = True,
    args_schema_non_parallel: bool = True,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[TaskReadV2]:
    """
    Get list of available tasks
    """
    stm = (
        select(TaskV2)
        .join(TaskGroupV2)
        .where(TaskGroupV2.id == TaskV2.taskgroupv2_id)
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
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> TaskReadV2:
    """
    Get info on a specific task
    """
    task = await _get_task_read_access(task_id=task_id, user_id=user.id, db=db)
    return task


@router.patch("/{task_id}/", response_model=TaskReadV2)
async def patch_task(
    task_id: int,
    task_update: TaskUpdateV2,
    user: UserOAuth = Depends(current_active_verified_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[TaskReadV2]:
    """
    Edit a specific task (restricted to superusers and task owner)
    """

    # Retrieve task from database
    db_task = await _get_task_full_access(
        task_id=task_id, user_id=user.id, db=db
    )
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
    user_group_id: Optional[int] = None,
    private: bool = False,
    user: UserOAuth = Depends(current_active_verified_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[TaskReadV2]:
    """
    Create a new task
    """

    # Validate query parameters related to user-group ownership
    user_group_id = await _get_valid_user_group_id(
        user_group_id=user_group_id,
        private=private,
        user_id=user.id,
        db=db,
    )

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

    # Set task.owner attribute - FIXME: remove this block
    if user.username:
        owner = user.username
    else:
        verify_user_has_settings(user)
        if user.settings.slurm_user:
            owner = user.settings.slurm_user
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

    db_task_group = TaskGroupV2(
        user_id=user.id,
        user_group_id=user_group_id,
        active=True,
        task_list=[db_task],
    )
    db.add(db_task_group)
    await db.commit()
    await db.refresh(db_task)
    await db.close()
    return db_task


@router.delete("/{task_id}/", status_code=204)
async def delete_task(
    task_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Response:
    """
    Delete a task
    """
    raise HTTPException(
        status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
        detail=(
            "Cannot delete single tasks, "
            "please operate directly on task groups."
        ),
    )
