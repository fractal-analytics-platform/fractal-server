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
from fractal_server.app.routes.auth import current_superuser_act
from fractal_server.app.routes.auth._aux_auth import (
    _verify_user_belongs_to_group,
)
from fractal_server.app.routes.pagination import PaginationRequest
from fractal_server.app.routes.pagination import PaginationResponse
from fractal_server.app.routes.pagination import get_pagination_params
from fractal_server.app.schemas.v2 import TaskGroupActivityAction
from fractal_server.app.schemas.v2 import TaskGroupActivityRead
from fractal_server.app.schemas.v2 import TaskGroupActivityStatus
from fractal_server.app.schemas.v2 import TaskGroupOriginEnum
from fractal_server.app.schemas.v2 import TaskGroupReadSuperuser
from fractal_server.app.schemas.v2 import TaskGroupUpdate
from fractal_server.logger import set_logger

router = APIRouter()

logger = set_logger(__name__)


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
    # Assign pagination parameters
    page = pagination.page
    page_size = pagination.page_size

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

    # Find total number of elements
    res_total_count = await db.execute(stm_count)
    total_count = res_total_count.scalar()
    if page_size is None:
        page_size = total_count
    else:
        stm = stm.offset((page - 1) * page_size).limit(page_size)

    res = await db.execute(stm)
    activities = res.scalars().all()

    return dict(
        total_count=total_count,
        page_size=page_size,
        current_page=page,
        items=activities,
    )


@router.get("/{task_group_id}/", response_model=TaskGroupReadSuperuser)
async def query_task_group(
    task_group_id: int,
    user: UserOAuth = Depends(current_superuser_act),
    db: AsyncSession = Depends(get_async_db),
) -> TaskGroupReadSuperuser:
    task_group = await db.get(TaskGroupV2, task_group_id)
    if task_group is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"TaskGroup {task_group_id} not found",
        )
    return task_group


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
    # Assign pagination parameters
    page = pagination.page
    page_size = pagination.page_size

    stm = select(TaskGroupV2)
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

    # Find total number of elements
    res_total_count = await db.execute(stm_count)
    total_count = res_total_count.scalar()
    if page_size is None:
        page_size = total_count
    else:
        stm = stm.offset((page - 1) * page_size).limit(page_size)

    stm = stm.order_by(TaskGroupV2.id)
    res = await db.execute(stm)
    task_groups_list = res.scalars().all()

    return dict(
        total_count=total_count,
        page_size=page_size,
        current_page=page,
        items=task_groups_list,
    )


@router.patch("/{task_group_id}/", response_model=TaskGroupReadSuperuser)
async def patch_task_group(
    task_group_id: int,
    task_group_update: TaskGroupUpdate,
    user: UserOAuth = Depends(current_superuser_act),
    db: AsyncSession = Depends(get_async_db),
) -> list[TaskGroupReadSuperuser]:
    task_group = await db.get(TaskGroupV2, task_group_id)
    if task_group is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"TaskGroupV2 {task_group_id} not found",
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
    return task_group
