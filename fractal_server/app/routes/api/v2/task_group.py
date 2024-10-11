from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from sqlmodel import or_
from sqlmodel import select

from ._aux_functions_tasks import _get_task_group_full_access
from ._aux_functions_tasks import _get_task_group_read_access
from ._aux_functions_tasks import _verify_non_duplication_group_constraint
from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import CollectionStateV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.app.routes.auth import current_active_user
from fractal_server.app.routes.auth._aux_auth import (
    _verify_user_belongs_to_group,
)
from fractal_server.app.schemas.v2 import TaskGroupReadV2
from fractal_server.app.schemas.v2 import TaskGroupUpdateV2
from fractal_server.logger import set_logger

router = APIRouter()

logger = set_logger(__name__)


@router.get("/", response_model=list[TaskGroupReadV2])
async def get_task_group_list(
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
    only_active: bool = False,
    only_owner: bool = False,
) -> list[TaskGroupReadV2]:
    """
    Get all accessible TaskGroups
    """

    if only_owner:
        condition = TaskGroupV2.user_id == user.id
    else:
        condition = or_(
            TaskGroupV2.user_id == user.id,
            TaskGroupV2.user_group_id.in_(
                select(LinkUserGroup.group_id).where(
                    LinkUserGroup.user_id == user.id
                )
            ),
        )
    stm = select(TaskGroupV2).where(condition)
    if only_active:
        stm = stm.where(TaskGroupV2.active)

    res = await db.execute(stm)
    task_groups = res.scalars().all()

    return task_groups


@router.get("/{task_group_id}/", response_model=TaskGroupReadV2)
async def get_task_group(
    task_group_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> TaskGroupReadV2:
    """
    Get single TaskGroup
    """
    task_group = await _get_task_group_read_access(
        task_group_id=task_group_id,
        user_id=user.id,
        db=db,
    )
    return task_group


@router.delete("/{task_group_id}/", status_code=204)
async def delete_task_group(
    task_group_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
):
    """
    Delete single TaskGroup
    """

    task_group = await _get_task_group_full_access(
        task_group_id=task_group_id,
        user_id=user.id,
        db=db,
    )

    stm = select(WorkflowTaskV2).where(
        WorkflowTaskV2.task_id.in_({task.id for task in task_group.task_list})
    )
    res = await db.execute(stm)
    workflow_tasks = res.scalars().all()
    if workflow_tasks != []:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"TaskV2 {workflow_tasks[0].task_id} is still in use",
        )

    # Cascade operations: set foreign-keys to null for CollectionStateV2 which
    # are in relationship with the current TaskGroupV2
    logger.debug("Start of cascade operations on CollectionStateV2.")
    stm = select(CollectionStateV2).where(
        CollectionStateV2.taskgroupv2_id == task_group_id
    )
    res = await db.execute(stm)
    collection_states = res.scalars().all()
    for collection_state in collection_states:
        logger.debug(
            f"Setting CollectionStateV2[{collection_state.id}].taskgroupv2_id "
            "to None."
        )
        collection_state.taskgroupv2_id = None
        db.add(collection_state)
    logger.debug("End of cascade operations on CollectionStateV2.")

    await db.delete(task_group)
    await db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch("/{task_group_id}/", response_model=TaskGroupReadV2)
async def patch_task_group(
    task_group_id: int,
    task_group_update: TaskGroupUpdateV2,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> TaskGroupReadV2:
    """
    Patch single TaskGroup
    """
    task_group = await _get_task_group_full_access(
        task_group_id=task_group_id,
        user_id=user.id,
        db=db,
    )
    if (
        "user_group_id" in task_group_update.dict(exclude_unset=True)
        and task_group_update.user_group_id != task_group.user_group_id
    ):
        await _verify_non_duplication_group_constraint(
            db=db,
            pkg_name=task_group.pkg_name,
            version=task_group.version,
            user_group_id=task_group_update.user_group_id,
        )
    for key, value in task_group_update.dict(exclude_unset=True).items():
        if (key == "user_group_id") and (value is not None):
            await _verify_user_belongs_to_group(
                user_id=user.id, user_group_id=value, db=db
            )
        setattr(task_group, key, value)

    db.add(task_group)
    await db.commit()
    await db.refresh(task_group)
    return task_group
