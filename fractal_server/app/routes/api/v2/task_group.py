import itertools
from typing import Any

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status
from pydantic.types import AwareDatetime
from sqlmodel import or_
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.routes.auth import get_api_guest
from fractal_server.app.routes.auth import get_api_user
from fractal_server.app.routes.auth._aux_auth import (
    _get_default_usergroup_id_or_none,
)
from fractal_server.app.routes.auth._aux_auth import (
    _verify_user_belongs_to_group,
)
from fractal_server.app.routes.aux._versions import _version_sort_key
from fractal_server.app.schemas.v2 import TaskGroupActivityAction
from fractal_server.app.schemas.v2 import TaskGroupActivityRead
from fractal_server.app.schemas.v2 import TaskGroupActivityStatus
from fractal_server.app.schemas.v2 import TaskGroupRead
from fractal_server.app.schemas.v2 import TaskGroupReadSlim
from fractal_server.app.schemas.v2 import TaskGroupUpdate
from fractal_server.logger import set_logger

from ._aux_functions import _get_user_resource_id
from ._aux_functions_tasks import _get_task_group_full_access
from ._aux_functions_tasks import _get_task_group_read_access
from ._aux_functions_tasks import _verify_non_duplication_group_constraint
from ._aux_task_group_disambiguation import add_user_email_to_task_group
from ._aux_task_group_disambiguation import remove_duplicate_task_groups
from ._aux_task_group_disambiguation import serialize_task_group_with_email
from .task import _SLIM_TASK_FIELDS

router = APIRouter()

logger = set_logger(__name__)


@router.get(
    "/activity/",
    response_model=list[TaskGroupActivityRead],
)
async def get_task_group_activity_list(
    task_group_activity_id: int | None = None,
    taskgroupv2_id: int | None = None,
    pkg_name: str | None = None,
    status: TaskGroupActivityStatus | None = None,
    action: TaskGroupActivityAction | None = None,
    timestamp_started_min: AwareDatetime | None = None,
    user: UserOAuth = Depends(get_api_guest),
    db: AsyncSession = Depends(get_async_db),
) -> list[TaskGroupActivityV2]:
    stm = select(TaskGroupActivityV2).where(
        TaskGroupActivityV2.user_id == user.id
    )
    if task_group_activity_id is not None:
        stm = stm.where(TaskGroupActivityV2.id == task_group_activity_id)
    if taskgroupv2_id is not None:
        stm = stm.where(TaskGroupActivityV2.taskgroupv2_id == taskgroupv2_id)
    if pkg_name is not None:
        stm = stm.where(TaskGroupActivityV2.pkg_name.icontains(pkg_name))
    if status is not None:
        stm = stm.where(TaskGroupActivityV2.status == status)
    if action is not None:
        stm = stm.where(TaskGroupActivityV2.action == action)
    if timestamp_started_min is not None:
        stm = stm.where(
            TaskGroupActivityV2.timestamp_started >= timestamp_started_min
        )

    res = await db.execute(stm)
    activities = res.scalars().all()
    return activities


@router.get(
    "/activity/{task_group_activity_id}/",
    response_model=TaskGroupActivityRead,
)
async def get_task_group_activity(
    task_group_activity_id: int,
    user: UserOAuth = Depends(get_api_guest),
    db: AsyncSession = Depends(get_async_db),
) -> TaskGroupActivityV2:
    activity = await db.get(TaskGroupActivityV2, task_group_activity_id)

    if activity is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"TaskGroupActivityV2 {task_group_activity_id} not found",
        )
    if activity.user_id != user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                "You are not the owner of TaskGroupActivityV2 "
                f"{task_group_activity_id}",
            ),
        )

    return activity


@router.get(
    "/",
    response_model=list[tuple[str, list[TaskGroupRead]]]
    | list[tuple[str, list[TaskGroupReadSlim]]],
)
async def get_task_group_list(
    user: UserOAuth = Depends(get_api_guest),
    db: AsyncSession = Depends(get_async_db),
    only_active: bool = False,
    only_owner: bool = False,
    slim: bool = False,
) -> list[tuple[str, list[dict[str, Any]]]]:
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

    user_resource_id = await _get_user_resource_id(user_id=user.id, db=db)
    stm = (
        select(TaskGroupV2, UserOAuth.email)
        .join(UserOAuth, UserOAuth.id == TaskGroupV2.user_id)
        .where(TaskGroupV2.resource_id == user_resource_id)
        .where(condition)
        .order_by(TaskGroupV2.pkg_name)
    )
    if only_active:
        stm = stm.where(TaskGroupV2.active)

    res = await db.execute(stm)
    task_groups_and_email = res.all()

    task_groups = [item[0] for item in task_groups_and_email]
    task_group_id_email_map = {
        task_group.id: user_email
        for task_group, user_email in task_groups_and_email
    }

    default_group_id = await _get_default_usergroup_id_or_none(db)
    grouped_result = [
        (
            pkg_name,
            (
                await remove_duplicate_task_groups(
                    task_groups=sorted(
                        list(groups),
                        key=lambda group: _version_sort_key(group.version),
                        reverse=True,
                    ),
                    user_id=user.id,
                    default_group_id=default_group_id,
                    db=db,
                )
            ),
        )
        for pkg_name, groups in itertools.groupby(
            task_groups, key=lambda tg: tg.pkg_name
        )
    ]
    grouped_result_with_emails = [
        (
            pkg_name,
            [
                serialize_task_group_with_email(
                    task_group=task_group,
                    user_email=task_group_id_email_map[task_group.id],
                    included_task_fields=(_SLIM_TASK_FIELDS if slim else None),
                )
                for task_group in task_group_list
            ],
        )
        for pkg_name, task_group_list in grouped_result
    ]

    return grouped_result_with_emails


@router.get("/{task_group_id}/", response_model=TaskGroupRead)
async def get_task_group(
    task_group_id: int,
    user: UserOAuth = Depends(get_api_guest),
    db: AsyncSession = Depends(get_async_db),
) -> TaskGroupV2:
    """
    Get single TaskGroup
    """
    task_group = await _get_task_group_read_access(
        task_group_id=task_group_id,
        user_id=user.id,
        db=db,
    )
    task_group_with_email = await add_user_email_to_task_group(
        task_group=task_group, db=db
    )
    return task_group_with_email


@router.patch("/{task_group_id}/", response_model=TaskGroupRead)
async def patch_task_group(
    task_group_id: int,
    task_group_update: TaskGroupUpdate,
    user: UserOAuth = Depends(get_api_user),
    db: AsyncSession = Depends(get_async_db),
) -> TaskGroupV2:
    """
    Patch single TaskGroup
    """
    task_group = await _get_task_group_full_access(
        task_group_id=task_group_id,
        user_id=user.id,
        db=db,
    )
    if (
        "user_group_id" in task_group_update.model_dump(exclude_unset=True)
        and task_group_update.user_group_id != task_group.user_group_id
    ):
        await _verify_non_duplication_group_constraint(
            db=db,
            pkg_name=task_group.pkg_name,
            version=task_group.version,
            user_group_id=task_group_update.user_group_id,
        )
    for key, value in task_group_update.model_dump(exclude_unset=True).items():
        if (key == "user_group_id") and (value is not None):
            await _verify_user_belongs_to_group(
                user_id=user.id, user_group_id=value, db=db
            )
        setattr(task_group, key, value)

    db.add(task_group)
    await db.commit()
    await db.refresh(task_group)
    task_group_with_email = await add_user_email_to_task_group(
        task_group=task_group, db=db
    )
    return task_group_with_email
