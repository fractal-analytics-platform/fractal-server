from copy import deepcopy  # noqa
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from sqlmodel import func
from sqlmodel import or_
from sqlmodel import select

from ._aux_functions_tasks import _get_task_full_access
from ._aux_functions_tasks import _get_task_read_access
from ._aux_functions_tasks import _get_valid_user_group_id
from ._aux_functions_tasks import _verify_non_duplication_group_constraint
from ._aux_functions_tasks import _verify_non_duplication_user_constraint
from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.routes.auth import current_active_user
from fractal_server.app.routes.auth import current_active_verified_user
from fractal_server.app.schemas.v2 import TaskCreateV2
from fractal_server.app.schemas.v2 import TaskGroupV2OriginEnum
from fractal_server.app.schemas.v2 import TaskReadV2
from fractal_server.app.schemas.v2 import TaskUpdateV2
from fractal_server.logger import set_logger

router = APIRouter()

logger = set_logger(__name__)


@router.get("/", response_model=list[TaskReadV2])
async def get_list_task(
    args_schema: bool = True,
    category: Optional[str] = None,
    modality: Optional[str] = None,
    author: Optional[str] = None,
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
    if category is not None:
        stm = stm.where(func.lower(TaskV2.category) == category.lower())
    if modality is not None:
        stm = stm.where(func.lower(TaskV2.modality) == modality.lower())
    if author is not None:
        stm = stm.where(TaskV2.authors.icontains(author))

    res = await db.execute(stm)
    task_list = res.scalars().all()
    await db.close()
    if args_schema is False:
        for task in task_list:
            setattr(task, "args_schema_parallel", None)
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
    Edit a specific task (restricted to task owner)
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

    # Add task
    db_task = TaskV2(**task.dict(), type=task_type)
    pkg_name = db_task.name
    await _verify_non_duplication_user_constraint(
        db=db, pkg_name=pkg_name, user_id=user.id, version=db_task.version
    )
    await _verify_non_duplication_group_constraint(
        db=db,
        pkg_name=pkg_name,
        user_group_id=user_group_id,
        version=db_task.version,
    )
    db_task_group = TaskGroupV2(
        user_id=user.id,
        user_group_id=user_group_id,
        active=True,
        task_list=[db_task],
        origin=TaskGroupV2OriginEnum.OTHER,
        version=db_task.version,
        pkg_name=pkg_name,
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
