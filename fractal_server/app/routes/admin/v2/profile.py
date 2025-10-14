from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from pydantic import ValidationError
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import Profile
from fractal_server.app.models.v2 import Resource
from fractal_server.app.routes.auth import current_active_superuser
from fractal_server.app.schemas.v2 import ProfileCreate
from fractal_server.app.schemas.v2 import ProfileRead
from fractal_server.app.schemas.v2 import ResourceType
from fractal_server.app.schemas.v2 import ValidProfileLocal
from fractal_server.app.schemas.v2 import ValidProfileSlurmSSH
from fractal_server.app.schemas.v2 import ValidProfileSlurmSudo

router = APIRouter()


async def _get_resource(*, resource_id: int, db: AsyncSession) -> Resource:
    resource = await db.get(Resource, resource_id)
    if resource is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Resource {resource_id} not found",
        )
    return resource


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
    await _get_resource(resource_id=resource_id, db=db)

    res = await db.execute(
        select(Profile).where(Profile.resource_id == resource_id)
    )
    profiles = res.scalars().all()

    return profiles


@router.get(
    "/{resource_id}/profile/{profile_id}/",
    response_model=ProfileRead,
    status_code=200,
)
async def get_single_profile(
    resource_id: int,
    profile_id: int,
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> list[ProfileRead]:
    """
    Query single `Profile`.
    """

    res = await db.execute(
        select(Profile)
        .join(Resource)
        .where(Resource.id == resource_id)
        .where(Profile.id == profile_id)
        .where(Profile.resource_id == Resource.id)
    )
    profile = res.scalars().one_or_none()

    if profile is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"Profile {profile_id} for Resource {resource_id} not found"
            ),
        )

    return profile


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
    resource = await _get_resource(resource_id=resource_id, db=db)

    profile = Profile(
        resource_id=resource_id,
        **profile_create.model_dump(),
    )

    try:
        match resource.type:
            case ResourceType.LOCAL:
                ValidProfileLocal(**profile.model_dump())
            case ResourceType.SLURM_SUDO:
                ValidProfileSlurmSudo(**profile.model_dump())
            case ResourceType.SLURM_SSH:
                ValidProfileSlurmSSH(**profile.model_dump())
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"Invalid profile for {resource.type=}. Original error: {e}"
            ),
        )

    db.add(profile)
    await db.commit()
    await db.refresh(profile)

    return profile


@router.patch(
    "/{resource_id}/profile/{profile_id}/",
    response_model=ProfileRead,
    status_code=200,
)
async def patch_profile(
    resource_id: int,
    profile_id: int,
    profile_update: Profile,
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> ProfileRead:
    """
    Patch single `Profile`.
    """
    resource = await _get_resource(resource_id=resource_id, db=db)
    profile = await get_single_profile(
        resource_id=resource_id,
        profile_id=profile_id,
        superuser=superuser,
        db=db,
    )

    for key, value in profile_update.model_dump(exclude_unset=True).items():
        setattr(profile, key, value)
    try:
        _data = profile.model_dump()
        match resource.type:
            case ResourceType.LOCAL:
                ValidProfileLocal(**_data)
            case ResourceType.SLURM_SUDO:
                ValidProfileSlurmSudo(**_data)
            case ResourceType.SLURM_SSH:
                ValidProfileSlurmSSH(**_data)
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                "PATCH would lead to invalid profile. Original error: "
                f"{str(e)}."
            ),
        )

    await db.commit()
    await db.refresh(profile)

    return profile


@router.delete("/{resource_id}/profile/{profile_id}/", status_code=204)
async def delete_profile(
    resource_id: int,
    profile_id: int,
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
):
    """
    Delete single `Profile`.
    """
    profile = await get_single_profile(
        resource_id=resource_id,
        profile_id=profile_id,
        superuser=superuser,
        db=db,
    )
    await db.delete(profile)
    await db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)
