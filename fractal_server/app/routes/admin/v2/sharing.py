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
from fractal_server.app.schemas.v2 import ProjectShareReadAdmin

router = APIRouter()


@router.get("/", response_model=PaginationResponse[ProjectShareReadAdmin])
async def view_link_user_project(
    project_id: int | None = None,
    owner_id: int | None = None,
    guest_id: int | None = None,
    is_owner: bool | None = None,
    is_verified: bool | None = None,
    pagination: PaginationRequest = Depends(get_pagination_params),
    user: UserOAuth = Depends(current_superuser_act),
    db: AsyncSession = Depends(get_async_db),
) -> PaginationResponse[ProjectShareReadAdmin]:
    page = pagination.page
    page_size = pagination.page_size

    stm = (
        select(
            ProjectV2.id,
            ProjectV2.name,
            (
                select(UserOAuth.id, UserOAuth.email)
                .join(
                    LinkUserProjectV2,
                    UserOAuth.id == LinkUserProjectV2.user_id,
                )
                .where(LinkUserProjectV2.is_owner.is_(True))
                .where(LinkUserProjectV2.project_id == ProjectV2.id)
                .scalar_subquery()
                .correlate(ProjectV2)
            ),
            LinkUserProjectV2.user_id,
            # UserOAuth.email,
            # LinkUserProjectV2.is_owner,
            # LinkUserProjectV2.is_verified,
            # LinkUserProjectV2.permissions,
        )
        .join(LinkUserProjectV2, LinkUserProjectV2.project_id == ProjectV2.id)
        .join(UserOAuth, UserOAuth.id == LinkUserProjectV2.user_id)
        .order_by(LinkUserProjectV2.user_id)
    )
    stm_count = select(LinkUserProjectV2.is_owner)

    stm = select(LinkUserProjectV2).order_by(LinkUserProjectV2.user_id)
    stm_count = select(func.count(LinkUserProjectV2.user_id))
    if project_id is not None:
        stm = stm.where(LinkUserProjectV2.project_id == project_id)
        stm_count = stm_count.where(LinkUserProjectV2.project_id == project_id)
    if guest_id is not None:
        stm = stm.where(LinkUserProjectV2.user_id == guest_id)
        stm_count = stm_count.where(LinkUserProjectV2.user_id == guest_id)
    if is_owner is not None:
        stm = stm.where(LinkUserProjectV2.is_owner == is_owner)
        stm_count = stm_count.where(LinkUserProjectV2.is_owner == is_owner)
    if is_verified is not None:
        stm = stm.where(LinkUserProjectV2.is_verified == is_verified)
        stm_count = stm_count.where(
            LinkUserProjectV2.is_verified == is_verified
        )

    # Find total number of elements
    res_total_count = await db.execute(stm_count)
    total_count = len(res_total_count.scalars().all())
    if page_size is None:
        page_size = total_count
    else:
        stm = stm.offset((page - 1) * page_size).limit(page_size)

    # Get `page_size` rows
    res = await db.execute(stm)
    items = res.all()

    return PaginationResponse[ProjectShareReadAdmin](
        total_count=total_count,
        page_size=page_size,
        current_page=page,
        items=[
            dict(
                project_id=project_id,
                project_name=project_name,
                owner_id=owner_id,
                owner_email=owner_email,
                guest_id=guest_id,
                guest_email=guest_email,
                is_owner=is_owner,
                is_verifed=is_verifed,
                permissions=permissions,
            )
            for (
                project_id,
                project_name,
                owner_id,
                owner_email,
                guest_id,
                guest_email,
                is_owner,
                is_verifed,
                permissions,
            ) in items
        ],
    )
