from fastapi import APIRouter
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from . import current_active_superuser
from ...db import get_async_db
from ._aux_auth import _user_or_404
from fractal_server.app.models import UserOAuth
from fractal_server.app.models import UserSettings
from fractal_server.app.schemas import UserSettingsRead
from fractal_server.app.schemas import UserSettingsUpdate
from fractal_server.logger import set_logger

router_users_settings = APIRouter()


logger = set_logger(__name__)


@router_users_settings.get(
    "/users/{user_id}/settings/", response_model=UserSettingsRead
)
async def get_user_settings(
    user_id: int,
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> UserSettingsRead:

    user = await _user_or_404(user_id=user_id, db=db)
    return await db.get(UserSettings, user.user_settings_id)


@router_users_settings.patch(
    "/users/{user_id}/settings/", response_model=UserSettingsRead
)
async def patch_user_settings(
    user_id: int,
    settings_update: UserSettingsUpdate,
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> UserSettingsRead:
    user = await _user_or_404(user_id=user_id, db=db)
    user_settings = await db.get(UserSettings, user.user_settings_id)

    for k, v in settings_update.dict(exclude_unset=True).items():
        setattr(user_settings, k, v)

    db.add(user_settings)
    await db.commit()
    await db.refresh(user_settings)

    return user_settings
