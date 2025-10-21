from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from sqlmodel import func
from sqlmodel import select

from ._aux_functions import _check_resource_name
from ._aux_functions import _get_resource_or_404
from .profile import _check_profile_name
from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import Profile
from fractal_server.app.models.v2 import ProjectV2
from fractal_server.app.models.v2 import Resource
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.routes.auth import current_active_superuser
from fractal_server.app.schemas.v2 import ProfileCreate
from fractal_server.app.schemas.v2 import ProfileRead
from fractal_server.app.schemas.v2 import ResourceCreate
from fractal_server.app.schemas.v2 import ResourceRead
from fractal_server.config import get_settings
from fractal_server.syringe import Inject

router = APIRouter()


def _check_resource_type_match_or_422(
    resource: Resource,
    new_profile: ProfileCreate,
) -> None:
    if resource.type != new_profile.resource_type:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"{resource.type=} differs from {new_profile.resource_type=}."
            ),
        )


def _check_type_match_or_422(new_resource: ResourceCreate) -> None:
    """
    Handle case where `resource.type != FRACTAL_RUNNER_BACKEND`
    """
    settings = Inject(get_settings)
    if settings.FRACTAL_RUNNER_BACKEND != new_resource.type:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"{settings.FRACTAL_RUNNER_BACKEND=} != "
                f"{new_resource.type=}"
            ),
        )


@router.get("/", response_model=list[ResourceRead], status_code=200)
async def get_resource_list(
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> list[ResourceRead]:
    """
    Query `Resource` table.
    """

    stm = select(Resource)
    res = await db.execute(stm)
    resource_list = res.scalars().all()

    return resource_list


@router.get("/{resource_id}/", response_model=ResourceRead, status_code=200)
async def get_resource(
    resource_id: int,
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> ResourceRead:
    """
    Query single `Resource`.
    """
    resource = await _get_resource_or_404(resource_id=resource_id, db=db)

    return resource


@router.post("/", response_model=ResourceRead, status_code=201)
async def post_resource(
    resource_create: ResourceCreate,
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> ResourceRead:
    """
    Create new `Resource`.
    """

    # Handle case where type!=FRACTAL_RUNNER_BACKEND
    _check_type_match_or_422(resource_create)

    await _check_resource_name(name=resource_create.name, db=db)

    resource = Resource(**resource_create.model_dump())
    db.add(resource)
    await db.commit()
    await db.refresh(resource)

    return resource


@router.put(
    "/{resource_id}/",
    response_model=ResourceRead,
    status_code=200,
)
async def put_resource(
    resource_id: int,
    resource_update: ResourceCreate,
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> ResourceRead:
    """
    Overwrite a single `Resource`.
    """

    # Handle case where type!=FRACTAL_RUNNER_BACKEND
    _check_type_match_or_422(resource_update)

    resource = await _get_resource_or_404(resource_id=resource_id, db=db)

    # Handle non-unique resource names
    if resource_update.name and resource_update.name != resource.name:
        await _check_resource_name(name=resource_update.name, db=db)

    # Prepare new db object
    for key, value in resource_update.model_dump().items():
        setattr(resource, key, value)

    await db.commit()
    await db.refresh(resource)
    return resource


@router.delete("/{resource_id}/", status_code=204)
async def delete_resource(
    resource_id: int,
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
):
    """
    Delete single `Resource`.
    """
    resource = await _get_resource_or_404(resource_id=resource_id, db=db)

    # Fail if at least one Profile is associated with the Resource.
    res = await db.execute(
        select(func.count(Profile.id)).where(
            Profile.resource_id == resource_id
        )
    )
    associated_profile_count = res.scalar()
    if associated_profile_count > 0:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"Cannot delete Resource {resource_id} because it's associated"
                f" with {associated_profile_count} Profiles."
            ),
        )

    # Fail if at least one Project is associated with the Resource.
    res = await db.execute(
        select(func.count(ProjectV2.id)).where(
            ProjectV2.resource_id == resource_id
        )
    )
    associated_project_count = res.scalar()
    if associated_project_count > 0:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"Cannot delete Resource {resource_id} because it's associated"
                f" with {associated_project_count} Projects."
            ),
        )

    # Fail if at least one TaskGroupV2 is associated with the Resource.
    res = await db.execute(
        select(func.count(TaskGroupV2.id)).where(
            TaskGroupV2.resource_id == resource_id
        )
    )
    associated_taskgroup_count = res.scalar()
    if associated_taskgroup_count > 0:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"Cannot delete Resource {resource_id} because it's associated"
                f" with {associated_taskgroup_count} TaskGroupV2."
            ),
        )

    # Delete
    await db.delete(resource)
    await db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get(
    "/{resource_id}/profile/",
    response_model=list[ProfileRead],
    status_code=200,
)
async def get_resource_profiles(
    resource_id: int,
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> list[ProfileRead]:
    """
    Query `Profile`s for single `Resource`.
    """
    await _get_resource_or_404(resource_id=resource_id, db=db)

    res = await db.execute(
        select(Profile).where(Profile.resource_id == resource_id)
    )
    profiles = res.scalars().all()

    return profiles


@router.post(
    "/{resource_id}/profile/",
    response_model=ProfileRead,
    status_code=201,
)
async def post_profile(
    resource_id: int,
    profile_create: ProfileCreate,
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> ProfileRead:
    """
    Create new `Profile`.
    """
    resource = await _get_resource_or_404(resource_id=resource_id, db=db)

    _check_resource_type_match_or_422(
        resource=resource,
        new_profile=profile_create,
    )
    await _check_profile_name(name=profile_create.name, db=db)

    profile = Profile(
        resource_id=resource_id,
        **profile_create.model_dump(),
    )

    db.add(profile)
    await db.commit()
    await db.refresh(profile)
    return profile
