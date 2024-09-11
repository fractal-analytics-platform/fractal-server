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
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import col
from sqlmodel import func
from sqlmodel import select

from . import current_active_superuser
from ...db import get_async_db
from ...schemas.user import UserRead
from ...schemas.user import UserUpdate
from ...schemas.user import UserUpdateWithNewGroupIds
from ._aux_auth import _get_single_user_with_group_ids
from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import UserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth._aux_auth import _user_or_404
from fractal_server.app.security import get_user_manager
from fractal_server.app.security import UserManager
from fractal_server.logger import set_logger

router_users = APIRouter()


logger = set_logger(__name__)


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
    user_update: UserUpdateWithNewGroupIds,
    current_superuser: UserOAuth = Depends(current_active_superuser),
    user_manager: UserManager = Depends(get_user_manager),
    db: AsyncSession = Depends(get_async_db),
):
    """
    Custom version of the PATCH-user route from `fastapi-users`.

    In order to keep the fastapi-users logic in place (which is convenient to
    update user attributes), we split the endpoint into two branches. We either
    go through the fastapi-users-based attribute-update branch, or through the
    branch where we establish new user/group relationships.

    Note that we prevent making both changes at the same time, since it would
    be more complex to guarantee that endpoint error would leave the database
    in the same state as before the API call.
    """

    # We prevent simultaneous editing of both user attributes and user/group
    # associations
    user_update_dict_without_groups = user_update.dict(
        exclude_unset=True, exclude={"new_group_ids"}
    )
    edit_attributes = user_update_dict_without_groups != {}
    edit_groups = user_update.new_group_ids is not None
    if edit_attributes and edit_groups:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "Cannot modify both user attributes and group membership. "
                "Please make two independent PATCH calls"
            ),
        )

    # Check that user exists
    user_to_patch = await _user_or_404(user_id, db)

    if edit_groups:
        # Establish new user/group relationships

        # Check that all required groups exist
        # Note: The reason for introducing `col` is as in
        # https://sqlmodel.tiangolo.com/tutorial/where/#type-annotations-and-errors,
        stm = select(func.count()).where(
            col(UserGroup.id).in_(user_update.new_group_ids)
        )
        res = await db.execute(stm)
        number_matching_groups = res.scalar()
        if number_matching_groups != len(user_update.new_group_ids):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=(
                    "Not all requested groups (IDs: "
                    f"{user_update.new_group_ids}) exist."
                ),
            )

        for new_group_id in user_update.new_group_ids:
            link = LinkUserGroup(user_id=user_id, group_id=new_group_id)
            db.add(link)

        try:
            await db.commit()
        except IntegrityError as e:
            error_msg = (
                f"Cannot link groups with IDs {user_update.new_group_ids} "
                f"to user {user_id}. "
                "Likely reason: one of these links already exists.\n"
                f"Original error: {str(e)}"
            )
            logger.info(error_msg)
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=error_msg,
            )

        patched_user = user_to_patch

    elif edit_attributes:
        # Modify user attributes
        try:
            user_update_without_groups = UserUpdate(
                **user_update_dict_without_groups
            )
            user = await user_manager.update(
                user_update_without_groups,
                user_to_patch,
                safe=False,
                request=None,
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
    else:
        # Nothing to do, just continue
        patched_user = user_to_patch

    # Enrich user object with `group_ids` attribute
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

    # TODO: possible optimizations for this construction are listed in
    # https://github.com/fractal-analytics-platform/fractal-server/issues/1742
    for ind, user in enumerate(user_list):
        user_list[ind] = dict(
            user.model_dump(),
            group_ids=[
                link.group_id for link in links if link.user_id == user.id
            ],
        )
    return user_list
