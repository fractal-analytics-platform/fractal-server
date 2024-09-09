"""
Definition of `/auth/current-user/` endpoints
"""
from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status
from fastapi_users import exceptions
from fastapi_users import schemas
from fastapi_users.router.common import ErrorCode
from sqlalchemy.ext.asyncio import AsyncSession

from . import current_active_user
from ...db import get_async_db
from ...schemas.user import UserRead
from ...schemas.user import UserUpdate
from ...schemas.user import UserUpdateStrict
from ._aux_auth import _get_single_user_with_group_names
from fractal_server.app.models import UserOAuth
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
    update = UserUpdate(**user_update.dict(exclude_unset=True))

    try:
        user = await user_manager.update(update, current_user, safe=True)
    except exceptions.InvalidPasswordException as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "code": ErrorCode.UPDATE_USER_INVALID_PASSWORD,
                "reason": e.reason,
            },
        )
    patched_user = schemas.model_validate(UserOAuth, user)

    patched_user_with_groups = await _get_single_user_with_group_names(
        patched_user, db
    )
    return patched_user_with_groups
