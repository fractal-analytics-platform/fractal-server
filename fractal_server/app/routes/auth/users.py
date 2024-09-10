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
from sqlmodel import select

from . import current_active_superuser
from ...db import get_async_db
from ...schemas.user import UserRead
from ...schemas.user import UserUpdate
from ._aux_auth import _get_single_user_with_group_ids
from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth._aux_auth import _user_or_404
from fractal_server.app.security import get_user_manager
from fractal_server.app.security import UserManager

router_users = APIRouter()


@router_users.get("/users/{user_id}/", response_model=UserRead)
async def get_user(
    user_id: int,
    group_ids: bool = True,
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> UserRead:
    user = await _user_or_404(user_id, db)
    if group_ids:
        user_with_group_ids = await _get_single_user_with_group_ids(user, db)
        return user_with_group_ids
    else:
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

    user_to_patch = await _user_or_404(user_id, db)

    try:
        user = await user_manager.update(
            user_update, user_to_patch, safe=False, request=None
        )
        patched_user = schemas.model_validate(UserOAuth, user)
    except exceptions.InvalidPasswordException as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "code": ErrorCode.UPDATE_USER_INVALID_PASSWORD,
                "reason": e.reason,
            },
        )

    patched_user_with_group_ids = await _get_single_user_with_group_ids(
        patched_user, db
    )

    return patched_user_with_group_ids


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

    # FIXME GROUPS: this must be optimized
    for ind, user in enumerate(user_list):
        user_list[ind] = dict(
            user.model_dump(),
            group_ids=[
                link.group_id for link in links if link.user_id == user.id
            ],
        )
    return user_list
