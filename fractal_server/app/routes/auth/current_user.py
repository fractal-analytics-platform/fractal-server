"""
Definition of `/auth/current-user/` endpoints
"""
import os

from fastapi import APIRouter
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from fractal_server.app.db import get_async_db
from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import Profile
from fractal_server.app.models import Resource
from fractal_server.app.models import UserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth import current_user_act
from fractal_server.app.routes.auth import current_user_act_ver
from fractal_server.app.routes.auth._aux_auth import (
    _get_single_user_with_groups,
)
from fractal_server.app.schemas import UserProfileInfo
from fractal_server.app.schemas.user import UserRead
from fractal_server.app.schemas.user import UserUpdate
from fractal_server.app.schemas.user import UserUpdateStrict
from fractal_server.app.security import get_user_manager
from fractal_server.app.security import UserManager
from fractal_server.config import DataAuthScheme
from fractal_server.config import get_data_settings
from fractal_server.syringe import Inject

router_current_user = APIRouter()


@router_current_user.get("/current-user/", response_model=UserRead)
async def get_current_user(
    group_ids_names: bool = False,
    user: UserOAuth = Depends(current_user_act),
    db: AsyncSession = Depends(get_async_db),
):
    """
    Return current user
    """
    if group_ids_names is True:
        user_with_groups = await _get_single_user_with_groups(user, db)
        return user_with_groups
    else:
        return user


@router_current_user.patch("/current-user/", response_model=UserRead)
async def patch_current_user(
    user_update: UserUpdateStrict,
    current_user: UserOAuth = Depends(current_user_act),
    user_manager: UserManager = Depends(get_user_manager),
    db: AsyncSession = Depends(get_async_db),
):
    """
    Note: a user cannot patch their own password (as enforced within the
    `UserUpdateStrict` schema).
    """
    update = UserUpdate(**user_update.model_dump(exclude_unset=True))

    # NOTE: here it would be relevant to catch an `InvalidPasswordException`
    # (from `fastapi_users.exceptions`), if we were to allow users change
    # their own password

    user = await user_manager.update(update, current_user, safe=True)
    validated_user = UserOAuth.model_validate(user.model_dump())

    patched_user = await db.get(
        UserOAuth, validated_user.id, populate_existing=True
    )
    patched_user_with_groups = await _get_single_user_with_groups(
        patched_user, db
    )
    return patched_user_with_groups


@router_current_user.get(
    "/current-user/profile-info/",
    response_model=UserProfileInfo,
)
async def get_current_user_profile_info(
    current_user: UserOAuth = Depends(current_user_act),
    db: AsyncSession = Depends(get_async_db),
) -> UserProfileInfo:
    stm = (
        select(Resource, Profile)
        .join(UserOAuth)
        .where(Resource.id == Profile.resource_id)
        .where(Profile.id == UserOAuth.profile_id)
        .where(UserOAuth.id == current_user.id)
    )
    res = await db.execute(stm)
    db_data = res.one_or_none()
    if db_data is None:
        response_data = dict(has_profile=False)
    else:
        resource, profile = db_data
        response_data = dict(
            has_profile=True,
            resource_name=resource.name,
            profile_name=profile.name,
            username=profile.username,
        )

    return response_data


@router_current_user.get(
    "/current-user/allowed-viewer-paths/", response_model=list[str]
)
async def get_current_user_allowed_viewer_paths(
    current_user: UserOAuth = Depends(current_user_act_ver),
    db: AsyncSession = Depends(get_async_db),
) -> list[str]:
    """
    Returns the allowed viewer paths for current user, according to the
    selected FRACTAL_DATA_AUTH_SCHEME
    """

    data_settings = Inject(get_data_settings)

    authorized_paths = []

    if data_settings.FRACTAL_DATA_AUTH_SCHEME == DataAuthScheme.NONE:
        return authorized_paths

    # Append `project_dir` to the list of authorized paths
    authorized_paths.append(current_user.project_dir)

    # If auth scheme is "users-folders" and `slurm_user` is set,
    # build and append the user folder
    if (
        data_settings.FRACTAL_DATA_AUTH_SCHEME == DataAuthScheme.USERS_FOLDERS
        and current_user.profile_id is not None
    ):
        profile = await db.get(Profile, current_user.profile_id)
        if profile is not None and profile.username is not None:
            base_folder = data_settings.FRACTAL_DATA_BASE_FOLDER
            user_folder = os.path.join(base_folder, profile.username)
            authorized_paths.append(user_folder)

    if data_settings.FRACTAL_DATA_AUTH_SCHEME == DataAuthScheme.VIEWER_PATHS:
        # Returns the union of `viewer_paths` for all user's groups
        cmd = (
            select(UserGroup.viewer_paths)
            .join(LinkUserGroup)
            .where(LinkUserGroup.group_id == UserGroup.id)
            .where(LinkUserGroup.user_id == current_user.id)
        )
        res = await db.execute(cmd)
        viewer_paths_nested = res.scalars().all()

        # Flatten a nested object and make its elements unique
        all_viewer_paths_set = {
            path
            for _viewer_paths in viewer_paths_nested
            for path in _viewer_paths
        }
        authorized_paths.extend(all_viewer_paths_set)

    return authorized_paths
