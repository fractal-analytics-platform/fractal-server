from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import Profile
from fractal_server.app.models.v2 import Resource
from fractal_server.app.routes.auth import current_active_superuser
from fractal_server.app.schemas.v2 import ResourceRead

router = APIRouter()


@router.get(
    "/resource-of-profile/{profile_id}/",
    response_model=ResourceRead,
    status_code=200,
)
async def get_resource_of_profile(
    profile_id: int,
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> ResourceRead:
    """
    Get the single `Resource` associated to a given profile.
    """
    res = await db.execute(
        select(Resource)
        .join(Profile)
        .where(Profile.id == profile_id)
        .where(Profile.resource_id == Resource.id)
    )
    resource = res.scalars().one_or_none()
    if resource is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No resource associated to {profile_id=}.",
        )
    return resource
