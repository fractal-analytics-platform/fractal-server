from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from . import current_active_superuser
from ...db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models import UserSettings
from fractal_server.app.schemas import SettingsRead
from fractal_server.app.schemas import SettingsUpdate
from fractal_server.logger import set_logger

router_users_settings = APIRouter()


logger = set_logger(__name__)


@router_users_settings.get(
    "/users/{user_id}/settings/", response_model=SettingsRead
)
async def get_user_settings(
    user_id: int,
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> SettingsRead:

    stm = (
        select(UserSettings)
        .join(UserOAuth)
        .where(UserOAuth.id == user_id)
        .where(UserOAuth.user_settings_id == UserSettings.id)
    )
    res = await db.execute(stm)
    user_settings = res.scalars().one_or_none()
    await db.close()

    if user_settings is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Settings for User {user_id}  not found.",
        )

    return user_settings


@router_users_settings.patch(
    "/users/{user_id}/settings/", response_model=SettingsRead
)
async def patch_user_settings(
    user_id: int,
    settings_update: SettingsUpdate,
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> SettingsRead:

    user_settings = await get_user_settings(
        user_id=user_id, superuser=superuser, db=db
    )
    for k, v in settings_update.dict(exclude_unset=True).items():
        setattr(user_settings, k, v)

    db.add(user_settings)
    await db.commit()
    await db.refresh(user_settings)
    await db.close()

    return user_settings
