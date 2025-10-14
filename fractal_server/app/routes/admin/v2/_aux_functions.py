from fastapi import HTTPException
from fastapi import status
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.models.v2 import Profile
from fractal_server.app.models.v2 import Resource


async def _get_resource_or_404(
    *,
    resource_id: int,
    db: AsyncSession,
) -> Resource:
    resource = await db.get(Resource, resource_id)
    if resource is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Resource {resource_id} not found",
        )
    return resource


async def _get_profile_or_404(
    *,
    resource_id: int,
    profile_id: int,
    db: AsyncSession,
) -> Profile:
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
