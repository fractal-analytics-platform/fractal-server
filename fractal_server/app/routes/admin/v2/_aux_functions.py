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
    profile_id: int,
    db: AsyncSession,
) -> Profile:
    profile = await db.get(Profile, profile_id)
    if profile is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Profile {profile_id} not found",
        )
    return profile


async def _check_profile_name(*, name: str, db: AsyncSession) -> None:
    res = await db.execute(select(Profile).where(Profile.name == name))
    namesake = res.scalars().one_or_none()
    if namesake is not None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"Profile with name '{name}' already exists.",
        )


async def _check_resource_name(*, name: str, db: AsyncSession) -> None:
    res = await db.execute(select(Resource).where(Resource.name == name))
    namesake = res.scalars().one_or_none()
    if namesake is not None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"Resource with name '{name}' already exists.",
        )
