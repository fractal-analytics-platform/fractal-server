"""
Definition of `/auth/group/` routes
"""
from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import col
from sqlmodel import select

from . import current_active_superuser
from ...db import get_async_db
from ...schemas.user_group import UserGroupCreate
from ...schemas.user_group import UserGroupRead
from ...schemas.user_group import UserGroupUpdate
from ._aux_auth import _get_single_group_with_user_ids
from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import UserGroup
from fractal_server.app.models import UserOAuth


router_group = APIRouter()


@router_group.get(
    "/group/", response_model=list[UserGroupRead], status_code=200
)
async def get_list_user_groups(
    user_ids: bool = False,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> list[UserGroupRead]:
    """
    FIXME docstring
    """

    # Get all groups
    stm_all_groups = select(UserGroup)
    res = await db.execute(stm_all_groups)
    groups = res.scalars().all()

    if user_ids is True:
        # Get all user/group links
        stm_all_links = select(LinkUserGroup)
        res = await db.execute(stm_all_links)
        links = res.scalars().all()

        # FIXME GROUPS: this must be optimized
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
    """
    FIXME docstring
    """
    group = await _get_single_group_with_user_ids(group_id=group_id, db=db)
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
    """
    FIXME docstring
    """

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
    new_group = UserGroup(name=group_create.name)
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
    """
    FIXME docstring
    """

    # Check that all required users exist
    # Note: The reason for introducing `col` is as in
    # https://sqlmodel.tiangolo.com/tutorial/where/#type-annotations-and-errors,
    stm = select(UserOAuth).where(
        col(UserOAuth.id).in_(group_update.new_user_ids)
    )
    res = await db.execute(stm)
    matching_users = res.scalars().unique().all()
    if not len(matching_users) == len(group_update.new_user_ids):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"At least user with IDs {group_update.new_user_ids} "
                "does not exist."
            ),
        )

    # Add new users to existing group
    for user_id in group_update.new_user_ids:
        link = LinkUserGroup(user_id=user_id, group_id=group_id)
        db.add(link)
    await db.commit()

    updated_group = await _get_single_group_with_user_ids(
        group_id=group_id, db=db
    )

    return updated_group


@router_group.delete(
    "/group/{group_id}/", status_code=status.HTTP_405_METHOD_NOT_ALLOWED
)
async def delete_single_group(
    group_id: int,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> UserGroupRead:
    """
    FIXME docstring
    """
    raise HTTPException(
        status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
        detail=(
            "Deleting a user group is not allowed, as it may restrict "
            "previously-granted access.",
        ),
    )