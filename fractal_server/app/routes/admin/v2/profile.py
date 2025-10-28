from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from sqlmodel import func
from sqlmodel import select

from ._aux_functions import _check_profile_name
from ._aux_functions import _get_profile_or_404
from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import Profile
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth import current_superuser_act
from fractal_server.app.schemas.v2 import ProfileCreate
from fractal_server.app.schemas.v2 import ProfileRead

router = APIRouter()


@router.get("/{profile_id}/", response_model=ProfileRead, status_code=200)
async def get_single_profile(
    profile_id: int,
    superuser: UserOAuth = Depends(current_superuser_act),
    db: AsyncSession = Depends(get_async_db),
) -> ProfileRead:
    """
    Query single `Profile`.
    """
    profile = await _get_profile_or_404(profile_id=profile_id, db=db)
    return profile


@router.get("/", response_model=list[ProfileRead], status_code=200)
async def get_profile_list(
    superuser: UserOAuth = Depends(current_superuser_act),
    db: AsyncSession = Depends(get_async_db),
) -> ProfileRead:
    """
    Query single `Profile`.
    """
    res = await db.execute(select(Profile).order_by(Profile.id))
    profiles = res.scalars().all()
    return profiles


@router.put("/{profile_id}/", response_model=ProfileRead, status_code=200)
async def put_profile(
    profile_id: int,
    profile_update: ProfileCreate,
    superuser: UserOAuth = Depends(current_superuser_act),
    db: AsyncSession = Depends(get_async_db),
) -> ProfileRead:
    """
    Override single `Profile`.
    """
    profile = await _get_profile_or_404(profile_id=profile_id, db=db)

    if profile_update.name and profile_update.name != profile.name:
        await _check_profile_name(name=profile_update.name, db=db)

    for key, value in profile_update.model_dump().items():
        setattr(profile, key, value)
    await db.commit()
    await db.refresh(profile)
    return profile


@router.delete("/{profile_id}/", status_code=204)
async def delete_profile(
    profile_id: int,
    superuser: UserOAuth = Depends(current_superuser_act),
    db: AsyncSession = Depends(get_async_db),
):
    """
    Delete single `Profile`.
    """
    profile = await _get_profile_or_404(profile_id=profile_id, db=db)

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
