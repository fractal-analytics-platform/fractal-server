"""
Definition of `/auth/users/` routes
"""
from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status
from fastapi_users import exceptions
from fastapi_users import schemas
from fastapi_users.router.common import ErrorCode
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import func
from sqlmodel import select

from . import current_active_superuser
from ...db import get_async_db
from ...schemas.user import UserRead
from ...schemas.user import UserUpdate
from ..aux.validate_user_settings import verify_user_has_settings
from ._aux_auth import _get_default_usergroup_id
from ._aux_auth import _get_single_user_with_groups
from ._aux_auth import FRACTAL_DEFAULT_GROUP_NAME
from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import UserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.models import UserSettings
from fractal_server.app.routes.auth._aux_auth import _user_or_404
from fractal_server.app.schemas import UserSettingsRead
from fractal_server.app.schemas import UserSettingsUpdate
from fractal_server.app.schemas.user import UserUpdateGroups
from fractal_server.app.security import get_user_manager
from fractal_server.app.security import UserManager
from fractal_server.logger import set_logger

router_users = APIRouter()


logger = set_logger(__name__)


@router_users.get("/users/{user_id}/", response_model=UserRead)
async def get_user(
    user_id: int,
    group_ids_names: bool = True,
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> UserRead:
    user = await _user_or_404(user_id, db)
    if group_ids_names:
        user_with_groups = await _get_single_user_with_groups(user, db)
        return user_with_groups
    return user


@router_users.patch("/users/{user_id}/", response_model=UserRead)
async def patch_user(
    user_id: int,
    user_update: UserUpdate,
    current_superuser: UserOAuth = Depends(current_active_superuser),
    user_manager: UserManager = Depends(get_user_manager),
    db: AsyncSession = Depends(get_async_db),
):
    """
    Custom version of the PATCH-user route from `fastapi-users`.
    """

    # Check that user exists
    user_to_patch = await _user_or_404(user_id, db)

    # Modify user attributes
    try:
        user = await user_manager.update(
            user_update,
            user_to_patch,
            safe=False,
            request=None,
        )
        validated_user = schemas.model_validate(UserOAuth, user.model_dump())
        patched_user = await db.get(
            UserOAuth, validated_user.id, populate_existing=True
        )
    except exceptions.InvalidPasswordException as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "code": ErrorCode.UPDATE_USER_INVALID_PASSWORD,
                "reason": e.reason,
            },
        )
    except exceptions.UserAlreadyExists:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail=ErrorCode.UPDATE_USER_EMAIL_ALREADY_EXISTS,
        )

    # Enrich user object with `group_ids_names` attribute
    patched_user_with_groups = await _get_single_user_with_groups(
        patched_user, db
    )

    return patched_user_with_groups


@router_users.get("/users/", response_model=list[UserRead])
async def list_users(
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
):
    """
    Return list of all users
    """
    stm = select(UserOAuth)
    res = await db.execute(stm)
    user_list = res.scalars().unique().all()

    # Get all user/group links
    stm_all_links = select(LinkUserGroup)
    res = await db.execute(stm_all_links)
    links = res.scalars().all()

    # TODO: possible optimizations for this construction are listed in
    # https://github.com/fractal-analytics-platform/fractal-server/issues/1742
    for ind, user in enumerate(user_list):
        user_list[ind] = dict(
            **user.model_dump(),
            oauth_accounts=user.oauth_accounts,
            group_ids=[
                link.group_id for link in links if link.user_id == user.id
            ],
        )

    return user_list


@router_users.post("/users/{user_id}/set-groups/", response_model=UserRead)
async def set_user_groups(
    user_id: int,
    user_update: UserUpdateGroups,
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> UserRead:
    # Preliminary check that all objects exist in the db
    user = await _user_or_404(user_id=user_id, db=db)
    target_group_ids = user_update.group_ids
    stm = select(func.count(UserGroup.id)).where(
        UserGroup.id.in_(target_group_ids)
    )
    res = await db.execute(stm)
    count = res.scalar()
    if count != len(target_group_ids):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Some UserGroups in {target_group_ids} do not exist.",
        )

    # Check that default group is not being removed
    default_group_id = await _get_default_usergroup_id(db=db)
    if default_group_id not in target_group_ids:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Cannot remove user from "
                f"'{FRACTAL_DEFAULT_GROUP_NAME}' group.",
            ),
        )

    # Prepare lists of links to be removed
    res = await db.execute(
        select(LinkUserGroup)
        .where(LinkUserGroup.user_id == user_id)
        .where(LinkUserGroup.group_id.not_in(target_group_ids))
    )
    links_to_remove = res.scalars().all()

    # Prepare lists of links to be added
    res = await db.execute(
        select(LinkUserGroup.group_id)
        .where(LinkUserGroup.user_id == user_id)
        .where(LinkUserGroup.group_id.in_(target_group_ids))
    )
    ids_links_already_in = res.scalars().all()
    ids_links_to_add = set(target_group_ids) - set(ids_links_already_in)

    # Remove/create links as needed
    for link in links_to_remove:
        logger.info(
            f"Removing LinkUserGroup with {link.user_id=} "
            f"and {link.group_id=}."
        )
        await db.delete(link)
    for group_id in ids_links_to_add:
        logger.info(
            f"Creating new LinkUserGroup with {user_id=} " f"and {group_id=}."
        )
        db.add(LinkUserGroup(user_id=user_id, group_id=group_id))
    await db.commit()

    user_with_groups = await _get_single_user_with_groups(user, db)

    return user_with_groups


@router_users.get(
    "/users/{user_id}/settings/", response_model=UserSettingsRead
)
async def get_user_settings(
    user_id: int,
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> UserSettingsRead:
    user = await _user_or_404(user_id=user_id, db=db)
    verify_user_has_settings(user)
    user_settings = await db.get(UserSettings, user.user_settings_id)
    return user_settings


@router_users.patch(
    "/users/{user_id}/settings/", response_model=UserSettingsRead
)
async def patch_user_settings(
    user_id: int,
    settings_update: UserSettingsUpdate,
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> UserSettingsRead:
    user = await _user_or_404(user_id=user_id, db=db)
    verify_user_has_settings(user)
    user_settings = await db.get(UserSettings, user.user_settings_id)

    for k, v in settings_update.model_dump(exclude_unset=True).items():
        setattr(user_settings, k, v)

    db.add(user_settings)
    await db.commit()
    await db.refresh(user_settings)

    return user_settings
