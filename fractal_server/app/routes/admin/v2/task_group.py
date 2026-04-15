from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status
from pydantic.types import AwareDatetime
from sqlalchemy.sql.operators import is_
from sqlalchemy.sql.operators import is_not
from sqlmodel import func
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.routes.api.v2._aux_task_group_disambiguation import (
    serialize_task_group_with_email,
)
from fractal_server.app.routes.auth import current_superuser_act
from fractal_server.app.routes.auth._aux_auth import (
    _verify_user_belongs_to_group,
)
from fractal_server.app.routes.pagination import PaginationRequest
from fractal_server.app.routes.pagination import PaginationResponse
from fractal_server.app.routes.pagination import get_paginated_response
from fractal_server.app.routes.pagination import get_pagination_data
from fractal_server.app.routes.pagination import get_pagination_params
from fractal_server.app.schemas.v2 import TaskGroupActivityAction
from fractal_server.app.schemas.v2 import TaskGroupActivityRead
from fractal_server.app.schemas.v2 import TaskGroupActivityStatus
from fractal_server.app.schemas.v2 import TaskGroupOriginEnum
from fractal_server.app.schemas.v2 import TaskGroupReadSuperuser
from fractal_server.app.schemas.v2 import TaskGroupUpdate

router = APIRouter()


@router.get(
    "/activity/", response_model=PaginationResponse[TaskGroupActivityRead]
)
async def get_task_group_activity_list(
    task_group_activity_id: int | None = None,
    user_id: int | None = None,
    taskgroupv2_id: int | None = None,
    pkg_name: str | None = None,
    status: TaskGroupActivityStatus | None = None,
    action: TaskGroupActivityAction | None = None,
    timestamp_started_min: AwareDatetime | None = None,
    pagination: PaginationRequest = Depends(get_pagination_params),
    superuser: UserOAuth = Depends(current_superuser_act),
    db: AsyncSession = Depends(get_async_db),
) -> PaginationResponse[TaskGroupActivityRead]:
    stm = select(TaskGroupActivityV2).order_by(
        TaskGroupActivityV2.timestamp_started.desc()
    )
    stm_count = select(func.count(TaskGroupActivityV2.id))
    if task_group_activity_id is not None:
        stm = stm.where(TaskGroupActivityV2.id == task_group_activity_id)
        stm_count = stm_count.where(
            TaskGroupActivityV2.id == task_group_activity_id
        )
    if user_id:
        stm = stm.where(TaskGroupActivityV2.user_id == user_id)
        stm_count = stm_count.where(TaskGroupActivityV2.user_id == user_id)
    if taskgroupv2_id:
        stm = stm.where(TaskGroupActivityV2.taskgroupv2_id == taskgroupv2_id)
        stm_count = stm_count.where(
            TaskGroupActivityV2.taskgroupv2_id == taskgroupv2_id
        )
    if pkg_name:
        stm = stm.where(TaskGroupActivityV2.pkg_name.icontains(pkg_name))
        stm_count = stm_count.where(
            TaskGroupActivityV2.pkg_name.icontains(pkg_name)
        )
    if status:
        stm = stm.where(TaskGroupActivityV2.status == status)
        stm_count = stm_count.where(TaskGroupActivityV2.status == status)
    if action:
        stm = stm.where(TaskGroupActivityV2.action == action)
        stm_count = stm_count.where(TaskGroupActivityV2.action == action)
    if timestamp_started_min is not None:
        stm = stm.where(
            TaskGroupActivityV2.timestamp_started >= timestamp_started_min
        )
        stm_count = stm_count.where(
            TaskGroupActivityV2.timestamp_started >= timestamp_started_min
        )

    paginated_response = await get_paginated_response(
        stm=stm, stm_count=stm_count, pagination=pagination, db=db
    )
    return paginated_response


@router.get("/{task_group_id}/", response_model=TaskGroupReadSuperuser)
async def query_task_group(
    task_group_id: int,
    user: UserOAuth = Depends(current_superuser_act),
    db: AsyncSession = Depends(get_async_db),
) -> TaskGroupReadSuperuser:
    res = await db.execute(
        select(TaskGroupV2, UserOAuth.email)
        .join(UserOAuth, UserOAuth.id == TaskGroupV2.user_id)
        .where(TaskGroupV2.id == task_group_id)
    )
    task_group_and_email = res.one_or_none()
    if task_group_and_email is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"TaskGroup {task_group_id} not found",
        )
    task_group, user_email = task_group_and_email
    return serialize_task_group_with_email(
        task_group=task_group,
        user_email=user_email,
    )


@router.get("/", response_model=PaginationResponse[TaskGroupReadSuperuser])
async def query_task_group_list(
    user_id: int | None = None,
    user_group_id: int | None = None,
    private: bool | None = None,
    active: bool | None = None,
    pkg_name: str | None = None,
    origin: TaskGroupOriginEnum | None = None,
    timestamp_last_used_min: AwareDatetime | None = None,
    timestamp_last_used_max: AwareDatetime | None = None,
    resource_id: int | None = None,
    pagination: PaginationRequest = Depends(get_pagination_params),
    user: UserOAuth = Depends(current_superuser_act),
    db: AsyncSession = Depends(get_async_db),
) -> PaginationResponse[TaskGroupReadSuperuser]:
    stm = (
        select(TaskGroupV2, UserOAuth.email)
        .join(UserOAuth, UserOAuth.id == TaskGroupV2.user_id)
        .order_by(TaskGroupV2.id)
    )
    stm_count = select(func.count(TaskGroupV2.id))

    if user_group_id is not None and private is True:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                "Cannot get task groups with both "
                f"{user_group_id=} and {private=}."
            ),
        )
    if user_id is not None:
        stm = stm.where(TaskGroupV2.user_id == user_id)
        stm_count = stm_count.where(TaskGroupV2.user_id == user_id)
    if user_group_id is not None:
        stm = stm.where(TaskGroupV2.user_group_id == user_group_id)
        stm_count = stm_count.where(TaskGroupV2.user_group_id == user_group_id)
    if private is not None:
        if private is True:
            stm = stm.where(is_(TaskGroupV2.user_group_id, None))
            stm_count = stm_count.where(is_(TaskGroupV2.user_group_id, None))
        else:
            stm = stm.where(is_not(TaskGroupV2.user_group_id, None))
            stm_count = stm_count.where(is_not(TaskGroupV2.user_group_id, None))
    if active is not None:
        if active is True:
            stm = stm.where(is_(TaskGroupV2.active, True))
            stm_count = stm_count.where(is_(TaskGroupV2.active, True))
        else:
            stm = stm.where(is_(TaskGroupV2.active, False))
            stm_count = stm_count.where(is_(TaskGroupV2.active, False))
    if origin is not None:
        stm = stm.where(TaskGroupV2.origin == origin)
        stm_count = stm_count.where(TaskGroupV2.origin == origin)
    if pkg_name is not None:
        stm = stm.where(TaskGroupV2.pkg_name.icontains(pkg_name))
        stm_count = stm_count.where(TaskGroupV2.pkg_name.icontains(pkg_name))
    if timestamp_last_used_min is not None:
        stm = stm.where(
            TaskGroupV2.timestamp_last_used >= timestamp_last_used_min
        )
        stm_count = stm_count.where(
            TaskGroupV2.timestamp_last_used >= timestamp_last_used_min
        )
    if timestamp_last_used_max is not None:
        stm = stm.where(
            TaskGroupV2.timestamp_last_used <= timestamp_last_used_max
        )
        stm_count = stm_count.where(
            TaskGroupV2.timestamp_last_used <= timestamp_last_used_max
        )
    if resource_id is not None:
        stm = stm.where(TaskGroupV2.resource_id == resource_id)
        stm_count = stm_count.where(TaskGroupV2.resource_id == resource_id)

    stm, pagination_data = await get_pagination_data(
        stm=stm, stm_count=stm_count, pagination=pagination, db=db
    )

    res = await db.execute(stm)
    task_groups_list = [
        serialize_task_group_with_email(
            task_group=task_group, user_email=user_email
        )
        for task_group, user_email in res.all()
    ]

    return dict(items=task_groups_list, **pagination_data.model_dump())


@router.patch("/{task_group_id}/", response_model=TaskGroupReadSuperuser)
async def patch_task_group(
    task_group_id: int,
    task_group_update: TaskGroupUpdate,
    user: UserOAuth = Depends(current_superuser_act),
    db: AsyncSession = Depends(get_async_db),
) -> list[TaskGroupReadSuperuser]:
    res = await db.execute(
        select(TaskGroupV2, UserOAuth.email)
        .join(UserOAuth, UserOAuth.id == TaskGroupV2.user_id)
        .where(TaskGroupV2.id == task_group_id)
    )
    task_group_and_email = res.one_or_none()
    if task_group_and_email is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"TaskGroupV2 {task_group_id} not found",
        )
    task_group, user_email = task_group_and_email

    for key, value in task_group_update.model_dump(exclude_unset=True).items():
        if (key == "user_group_id") and (value is not None):
            await _verify_user_belongs_to_group(
                user_id=user.id, user_group_id=value, db=db
            )
        setattr(task_group, key, value)

    db.add(task_group)
    await db.commit()
    await db.refresh(task_group)
    return serialize_task_group_with_email(
        task_group=task_group,
        user_email=user_email,
    )
