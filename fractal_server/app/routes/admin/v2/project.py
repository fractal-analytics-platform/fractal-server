from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from sqlalchemy import func
from sqlmodel import and_
from sqlmodel import delete
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import LinkUserProjectV2
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.models.v2 import Profile
from fractal_server.app.models.v2 import ProjectV2
from fractal_server.app.models.v2 import Resource
from fractal_server.app.routes.auth import current_superuser_act
from fractal_server.app.routes.aux.validate_user_profile import (
    user_has_profile_or_422,
)
from fractal_server.app.routes.pagination import PaginationRequest
from fractal_server.app.routes.pagination import PaginationResponse
from fractal_server.app.routes.pagination import get_pagination_params
from fractal_server.app.schemas.v2 import ProjectReadSuperuser
from fractal_server.urls import url_is_relative_to

router = APIRouter()


@router.get("/", response_model=PaginationResponse[ProjectReadSuperuser])
async def view_projects(
    project_id: int | None = None,
    name: str | None = None,
    user_email: str | None = None,
    pagination: PaginationRequest = Depends(get_pagination_params),
    superuser: UserOAuth = Depends(current_superuser_act),
    db: AsyncSession = Depends(get_async_db),
) -> PaginationResponse[ProjectReadSuperuser]:
    # Assign pagination parameters
    page = pagination.page
    page_size = pagination.page_size

    # Prepare statements
    stm = (
        select(ProjectV2, UserOAuth.email)
        .join(LinkUserProjectV2, LinkUserProjectV2.project_id == ProjectV2.id)
        .where(LinkUserProjectV2.is_owner.is_(True))
        .join(UserOAuth, UserOAuth.id == LinkUserProjectV2.user_id)
        .order_by(UserOAuth.email, ProjectV2.name)
    )
    stm_count = select(func.count(ProjectV2.id))

    if project_id is not None:
        stm = stm.where(ProjectV2.id == project_id)
        stm_count = stm_count.where(ProjectV2.id == project_id)

    if name is not None:
        stm = stm.where(ProjectV2.name.icontains(name))
        stm_count = stm_count.where(ProjectV2.name.icontains(name))

    if user_email is not None:
        stm = stm.where(UserOAuth.email == user_email)
        stm_count = (
            stm_count.join(
                LinkUserProjectV2, LinkUserProjectV2.project_id == ProjectV2.id
            )
            .where(LinkUserProjectV2.is_owner.is_(True))
            .join(UserOAuth, UserOAuth.id == LinkUserProjectV2.user_id)
            .where(UserOAuth.email == user_email)
        )

    # Find total number of elements
    res_total_count = await db.execute(stm_count)
    total_count = res_total_count.scalar()
    if page_size is None:
        page_size = total_count
    else:
        stm = stm.offset((page - 1) * page_size).limit(page_size)

    res = await db.execute(stm)

    projects = [
        dict(user_email=email, **project.model_dump())
        for project, email in res.all()
    ]

    return dict(
        total_count=total_count,
        page_size=page_size,
        current_page=page,
        items=projects,
    )


@router.patch("/{project_id}/", status_code=status.HTTP_200_OK)
async def transfer_project_ownership(
    project_id: int,
    user_id: int,
    superuser: UserOAuth = Depends(current_superuser_act),
    db: AsyncSession = Depends(get_async_db),
) -> Response:
    # Get project
    project = await db.get(ProjectV2, project_id)
    if project is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project {project_id} not found.",
        )

    # Get new user
    new_user = await db.get(UserOAuth, user_id)
    if new_user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User {user_id} not found.",
        )
    await user_has_profile_or_422(user=new_user)
    if new_user.is_guest:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Cannot transfer ownership to a guest user.",
        )

    # Get old user and owner's link
    res = await db.execute(
        select(LinkUserProjectV2)
        .where(LinkUserProjectV2.project_id == project_id)
        .where(LinkUserProjectV2.is_owner.is_(True))
    )
    owner_link = res.scalar_one()
    if owner_link.user_id == user_id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"User {user_id} is already the owner of project {project_id}."
            ),
        )
    old_user = await db.get(UserOAuth, owner_link.user_id)
    await user_has_profile_or_422(user=old_user)

    # Check new user's resource compatibility
    res = await db.execute(
        select(Resource.name)
        .join(Profile, Profile.resource_id == Resource.id)
        .where(Profile.id.in_([old_user.profile_id, new_user.profile_id]))
    )
    resource_names = res.unique().scalars().all()
    if len(resource_names) > 1:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                "Users are associated to different computational resources "
                f"('{resource_names[0]}' and '{resource_names[1]}')."
            ),
        )

    # Check new user's project_dirs compatibility
    res = await db.execute(
        select(DatasetV2.zarr_dir).where(DatasetV2.project_id == project_id)
    )
    zarr_dirs = res.scalars().all()
    for zarr_dir in zarr_dirs:
        if all(
            not url_is_relative_to(url=zarr_dir, base=project_dir)
            for project_dir in new_user.project_dirs
        ):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail=(
                    "Cannot transfer project ownership because "
                    f"{zarr_dir=} is not relative to one of {new_user.email} "
                    "project dirs. "
                    f"You can fix it by editing {new_user.email} project dirs."
                ),
            )

    # Check new user is not already linked
    await db.execute(
        delete(LinkUserProjectV2).where(
            and_(
                LinkUserProjectV2.project_id == project_id,
                LinkUserProjectV2.user_id == user_id,
            )
        )
    )

    # Patch
    setattr(owner_link, "user_id", user_id)
    await db.commit()

    return Response(status_code=status.HTTP_200_OK)
