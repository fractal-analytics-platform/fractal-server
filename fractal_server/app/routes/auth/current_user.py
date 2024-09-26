"""
Definition of `/auth/current-user/` endpoints
"""
from fastapi import APIRouter
from fastapi import Depends
from fastapi_users import schemas
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from . import current_active_user
from ...db import get_async_db
from ...schemas.user import UserRead
from ...schemas.user import UserUpdate
from ...schemas.user import UserUpdateStrict
from ..aux.validate_user_settings import verify_user_has_settings
from ._aux_auth import _get_single_user_with_group_names
from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import UserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.models import UserSettings
from fractal_server.app.schemas import UserSettingsReadStrict
from fractal_server.app.schemas import UserSettingsUpdateStrict
from fractal_server.app.security import get_user_manager
from fractal_server.app.security import UserManager

router_current_user = APIRouter()


@router_current_user.get("/current-user/", response_model=UserRead)
async def get_current_user(
    group_names: bool = False,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
):
    """
    Return current user
    """
    if group_names is True:
        user_with_groups = await _get_single_user_with_group_names(user, db)
        return user_with_groups
    else:
        return user


@router_current_user.patch("/current-user/", response_model=UserRead)
async def patch_current_user(
    user_update: UserUpdateStrict,
    current_user: UserOAuth = Depends(current_active_user),
    user_manager: UserManager = Depends(get_user_manager),
    db: AsyncSession = Depends(get_async_db),
):
    """
    Note: a user cannot patch their own password (as enforced within the
    `UserUpdateStrict` schema).
    """
    update = UserUpdate(**user_update.dict(exclude_unset=True))

    # NOTE: here it would be relevant to catch an `InvalidPasswordException`
    # (from `fastapi_users.exceptions`), if we were to allow users change
    # their own password

    user = await user_manager.update(update, current_user, safe=True)
    validated_user = schemas.model_validate(UserOAuth, user)

    patched_user = await db.get(
        UserOAuth, validated_user.id, populate_existing=True
    )
    patched_user_with_groups = await _get_single_user_with_group_names(
        patched_user, db
    )
    return patched_user_with_groups


@router_current_user.get(
    "/current-user/settings/", response_model=UserSettingsReadStrict
)
async def get_current_user_settings(
    current_user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> UserSettingsReadStrict:

    verify_user_has_settings(current_user)
    user_settings = await db.get(UserSettings, current_user.user_settings_id)
    return user_settings


@router_current_user.patch(
    "/current-user/settings/", response_model=UserSettingsReadStrict
)
async def patch_current_user_settings(
    settings_update: UserSettingsUpdateStrict,
    current_user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> UserSettingsReadStrict:

    verify_user_has_settings(current_user)
    current_user_settings = await db.get(
        UserSettings, current_user.user_settings_id
    )

    for k, v in settings_update.dict(exclude_unset=True).items():
        setattr(current_user_settings, k, v)

    db.add(current_user_settings)
    await db.commit()
    await db.refresh(current_user_settings)

    return current_user_settings


@router_current_user.get(
    "/current-user/viewer-paths/", response_model=list[str]
)
async def get_current_user_viewer_paths(
    current_user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[str]:
    """Returns the union of `viewer_paths` for all user's groups"""
    cmd = (
        select(UserGroup.viewer_paths)
        .join(LinkUserGroup)
        .where(LinkUserGroup.group_id == UserGroup.id)
        .where(LinkUserGroup.user_id == current_user.id)
    )
    res = await db.execute(cmd)
    viewer_paths = list(set(res.scalars().all()))
    return viewer_paths