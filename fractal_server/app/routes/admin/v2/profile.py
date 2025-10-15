from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from sqlmodel import func
from sqlmodel import select

from ._aux_functions import _check_profile_name
from ._aux_functions import _get_profile_or_404
from ._aux_functions import _get_resource_or_404
from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import Profile
from fractal_server.app.models.v2 import Resource
from fractal_server.app.routes.auth import current_active_superuser
from fractal_server.app.schemas.v2 import ProfileCreate
from fractal_server.app.schemas.v2 import ProfileRead

router = APIRouter()


def _check_resource_type_match_or_422(
    resource: Resource,
    new_profile: ProfileCreate,
) -> None:
    if resource.type != new_profile.resource_type:
        raise HTTPException(
            status=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"{resource.type=} differs from {new_profile.resource_type=}."
            ),
        )


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
) -> ProfileRead:
    """
    Query single `Profile`.
    """
    profile = await _get_profile_or_404(
        resource_id=resource_id, profile_id=profile_id, db=db
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


@router.put(
    "/{resource_id}/profile/{profile_id}/",
    response_model=ProfileRead,
    status_code=200,
)
async def put_profile(
    resource_id: int,
    profile_id: int,
    profile_update: ProfileCreate,
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> ProfileRead:
    """
    Patch single `Profile`.
    """
    resource = await _get_resource_or_404(resource_id=resource_id, db=db)
    profile = await _get_profile_or_404(
        resource_id=resource_id,
        profile_id=profile_id,
        db=db,
    )
    _check_resource_type_match_or_422(
        resource=resource,
        new_profile=profile_update,
    )
    if profile_update.name and profile_update.name != profile.name:
        await _check_profile_name(name=profile_update.name, db=db)

    for key, value in profile_update.model_dump().items():
        setattr(profile, key, value)
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
    profile = await _get_profile_or_404(
        resource_id=resource_id,
        profile_id=profile_id,
        db=db,
    )

    # Fail if at least one UserOAuth is associated with the Profile.
    res = await db.execute(
        select(func.count(UserOAuth.id)).where(
            UserOAuth.profile_id == profile.id
        )
    )
    associated_users_count = res.scalar()
    if associated_users_count > 0:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"Cannot delete Profile {profile_id} because it's associated"
                f" with {associated_users_count} UserOAuths."
            ),
        )

    # Delete
    await db.delete(profile)
    await db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)
