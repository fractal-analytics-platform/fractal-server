"""
Definition of `/auth/group/` routes
"""
from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import col
from sqlmodel import func
from sqlmodel import select

from . import current_active_superuser
from ._aux_auth import _get_single_usergroup_with_user_ids
from ._aux_auth import _usergroup_or_404
from fractal_server.app.db import get_async_db
from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import UserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.models import UserSettings
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.schemas.user_group import UserGroupCreate
from fractal_server.app.schemas.user_group import UserGroupRead
from fractal_server.app.schemas.user_group import UserGroupUpdate
from fractal_server.app.schemas.user_settings import UserSettingsUpdate
from fractal_server.app.security import FRACTAL_DEFAULT_GROUP_NAME
from fractal_server.logger import set_logger

logger = set_logger(__name__)

router_group = APIRouter()


@router_group.get(
    "/group/", response_model=list[UserGroupRead], status_code=200
)
async def get_list_user_groups(
    user_ids: bool = False,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> list[UserGroupRead]:

    # Get all groups
    stm_all_groups = select(UserGroup)
    res = await db.execute(stm_all_groups)
    groups = res.scalars().all()

    if user_ids is True:
        # Get all user/group links
        stm_all_links = select(LinkUserGroup)
        res = await db.execute(stm_all_links)
        links = res.scalars().all()

        # TODO: possible optimizations for this construction are listed in
        # https://github.com/fractal-analytics-platform/fractal-server/issues/1742
        for ind, group in enumerate(groups):
            groups[ind] = dict(
                group.model_dump(),
                user_ids=[
                    link.user_id for link in links if link.group_id == group.id
                ],
            )

    return groups


@router_group.get(
    "/group/{group_id}/",
    response_model=UserGroupRead,
    status_code=status.HTTP_200_OK,
)
async def get_single_user_group(
    group_id: int,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> UserGroupRead:
    group = await _get_single_usergroup_with_user_ids(group_id=group_id, db=db)
    return group


@router_group.post(
    "/group/",
    response_model=UserGroupRead,
    status_code=status.HTTP_201_CREATED,
)
async def create_single_group(
    group_create: UserGroupCreate,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> UserGroupRead:

    # Check that name is not already in use
    existing_name_str = select(UserGroup).where(
        UserGroup.name == group_create.name
    )
    res = await db.execute(existing_name_str)
    group = res.scalars().one_or_none()
    if group is not None:
        raise HTTPException(
            status_code=422, detail="A group with the same name already exists"
        )

    # Create and return new group
    new_group = UserGroup(
        name=group_create.name, viewer_paths=group_create.viewer_paths
    )
    db.add(new_group)
    await db.commit()

    return dict(new_group.model_dump(), user_ids=[])


@router_group.patch(
    "/group/{group_id}/",
    response_model=UserGroupRead,
    status_code=status.HTTP_200_OK,
)
async def update_single_group(
    group_id: int,
    group_update: UserGroupUpdate,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> UserGroupRead:

    group = await _usergroup_or_404(group_id, db)

    # Check that all required users exist
    # Note: The reason for introducing `col` is as in
    # https://sqlmodel.tiangolo.com/tutorial/where/#type-annotations-and-errors,
    stm = select(func.count()).where(
        col(UserOAuth.id).in_(group_update.new_user_ids)
    )
    res = await db.execute(stm)
    number_matching_users = res.scalar()
    if number_matching_users != len(group_update.new_user_ids):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"Not all requested users (IDs {group_update.new_user_ids}) "
                "exist."
            ),
        )

    # Add new users to existing group
    for user_id in group_update.new_user_ids:
        link = LinkUserGroup(user_id=user_id, group_id=group_id)
        db.add(link)
    try:
        await db.commit()
    except IntegrityError as e:
        error_msg = (
            f"Cannot link users with IDs {group_update.new_user_ids} "
            f"to group {group_id}. "
            "Likely reason: one of these links already exists.\n"
            f"Original error: {str(e)}"
        )
        logger.info(error_msg)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=error_msg,
        )

    # Patch `viewer_paths`
    if group_update.viewer_paths is not None:
        group.viewer_paths = group_update.viewer_paths
        db.add(group)
        await db.commit()

    updated_group = await _get_single_usergroup_with_user_ids(
        group_id=group_id, db=db
    )

    return updated_group


@router_group.delete("/group/{group_id}/", status_code=204)
async def delete_single_group(
    group_id: int,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> Response:

    group = await _usergroup_or_404(group_id, db)

    if group.name == FRACTAL_DEFAULT_GROUP_NAME:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "Cannot delete default UserGroup "
                f"'{FRACTAL_DEFAULT_GROUP_NAME}'."
            ),
        )

    # Cascade operations

    res = await db.execute(
        select(LinkUserGroup).where(LinkUserGroup.group_id == group_id)
    )
    for link in res.scalars().all():
        await db.delete(link)

    res = await db.execute(
        select(TaskGroupV2).where(TaskGroupV2.user_group_id == group_id)
    )
    for task_group in res.scalars().all():
        task_group.user_group_id = None
        db.add(task_group)

    # Delete

    await db.delete(group)
    await db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router_group.patch("/group/{group_id}/user-settings/", status_code=200)
async def patch_user_settings_bulk(
    group_id: int,
    settings_update: UserSettingsUpdate,
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
):
    await _usergroup_or_404(group_id, db)
    res = await db.execute(
        select(UserSettings)
        .join(UserOAuth)
        .where(LinkUserGroup.user_id == UserOAuth.id)
        .where(LinkUserGroup.group_id == group_id)
    )
    settings_list = res.scalars().all()
    update = settings_update.dict(exclude_unset=True)
    for settings in settings_list:
        for k, v in update.items():
            setattr(settings, k, v)
        db.add(settings)
    await db.commit()

    return Response(status_code=status.HTTP_200_OK)
