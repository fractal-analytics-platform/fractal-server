from fastapi import APIRouter
from fastapi import Depends
from sqlalchemy import func
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import LinkUserProjectV2
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import ProjectV2
from fractal_server.app.routes.auth import current_superuser_act
from fractal_server.app.routes.pagination import PaginationRequest
from fractal_server.app.routes.pagination import PaginationResponse
from fractal_server.app.routes.pagination import get_pagination_params
from fractal_server.app.schemas.v2 import LinkUserProjectRead

router = APIRouter()


@router.get("/", response_model=PaginationResponse[LinkUserProjectRead])
async def view_link_user_project(
    # User info
    user_id: int | None = None,
    # Project info
    project_id: int | None = None,
    project_name: str | None = None,
    # Permissions
    is_owner: bool | None = None,
    is_verified: bool | None = None,
    # -----
    pagination: PaginationRequest = Depends(get_pagination_params),
    superuser: UserOAuth = Depends(current_superuser_act),
    db: AsyncSession = Depends(get_async_db),
) -> PaginationResponse[LinkUserProjectRead]:
    page = pagination.page
    page_size = pagination.page_size

    stm = (
        select(
            LinkUserProjectV2,
            UserOAuth.email,
            ProjectV2.name,
        )
        .join(UserOAuth, UserOAuth.id == LinkUserProjectV2.user_id)
        .join(ProjectV2, ProjectV2.id == LinkUserProjectV2.project_id)
        .order_by(UserOAuth.email, ProjectV2.name)
    )
    stm_count = (
        select(func.count())
        .select_from(LinkUserProjectV2)
        .join(UserOAuth, UserOAuth.id == LinkUserProjectV2.user_id)
        .join(ProjectV2, ProjectV2.id == LinkUserProjectV2.project_id)
    )

    if project_id is not None:
        stm = stm.where(LinkUserProjectV2.project_id == project_id)
        stm_count = stm_count.where(LinkUserProjectV2.project_id == project_id)
    if project_name is not None:
        stm = stm.where(ProjectV2.name.icontains(project_name))
        stm_count = stm_count.where(ProjectV2.name.icontains(project_name))
    if user_id is not None:
        stm = stm.where(LinkUserProjectV2.user_id == user_id)
        stm_count = stm_count.where(LinkUserProjectV2.user_id == user_id)
    if is_owner is not None:
        stm = stm.where(LinkUserProjectV2.is_owner == is_owner)
        stm_count = stm_count.where(LinkUserProjectV2.is_owner == is_owner)
    if is_verified is not None:
        stm = stm.where(LinkUserProjectV2.is_verified == is_verified)
        stm_count = stm_count.where(
            LinkUserProjectV2.is_verified == is_verified
        )

    res_total_count = await db.execute(stm_count)

    total_count = res_total_count.scalar()
    if page_size is None:
        page_size = total_count
    else:
        stm = stm.offset((page - 1) * page_size).limit(page_size)

    res = await db.execute(stm)
    items = res.all()

    return PaginationResponse[LinkUserProjectRead](
        total_count=total_count,
        page_size=page_size,
        current_page=page,
        items=[
            dict(
                # User info
                user_id=linkuserproject.user_id,
                user_email=user_email,
                # Project info
                project_id=linkuserproject.project_id,
                project_name=project_name,
                # Permissions
                is_verified=linkuserproject.is_verified,
                is_owner=linkuserproject.is_owner,
                permissions=linkuserproject.permissions,
            )
            for linkuserproject, user_email, project_name in items
        ],
    )
