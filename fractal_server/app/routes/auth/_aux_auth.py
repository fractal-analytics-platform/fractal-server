from fastapi import HTTPException
from fastapi import status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import asc
from sqlmodel import select

from fractal_server.app.models.linkusergroup import LinkUserGroup
from fractal_server.app.models.security import UserGroup
from fractal_server.app.models.security import UserOAuth
from fractal_server.app.schemas.user import UserRead
from fractal_server.app.schemas.user_group import UserGroupRead
from fractal_server.app.security import FRACTAL_DEFAULT_GROUP_NAME
from fractal_server.logger import set_logger

logger = set_logger(__name__)


async def _get_single_user_with_groups(
    user: UserOAuth,
    db: AsyncSession,
) -> UserRead:
    """
    Enrich a user object by filling its `group_ids_names` attribute.

    Arguments:
        user: The current `UserOAuth` object
        db: Async db session

    Returns:
        A `UserRead` object with `group_ids_names` dict
    """
    stm_groups = (
        select(UserGroup)
        .join(LinkUserGroup)
        .where(LinkUserGroup.user_id == user.id)
        .order_by(asc(LinkUserGroup.timestamp_created))
    )
    res = await db.execute(stm_groups)
    groups = res.scalars().unique().all()
    group_ids_names = [(group.id, group.name) for group in groups]

    # Check that Fractal Default Group is the first of the list. If not, fix.
    index = next(
        (
            i
            for i, group_tuple in enumerate(group_ids_names)
            if group_tuple[1] == FRACTAL_DEFAULT_GROUP_NAME
        ),
        None,
    )
    if index is None:
        logger.warning(
            f"User {user.id} not in "
            f"default UserGroup '{FRACTAL_DEFAULT_GROUP_NAME}'"
        )
    elif index != 0:
        default_group = group_ids_names.pop(index)
        group_ids_names.insert(0, default_group)
    else:
        pass
    oauth_accounts = [
        oauth_account.model_dump() for oauth_account in user.oauth_accounts
    ]

    return UserRead(
        **user.model_dump(),
        group_ids_names=group_ids_names,
        oauth_accounts=oauth_accounts,
    )


async def _get_single_usergroup_with_user_ids(
    group_id: int, db: AsyncSession
) -> UserGroupRead:
    """
    Get a group, and construct its `user_ids` list.

    Arguments:
        group_id:
        db:

    Returns:
        `UserGroupRead` object, with `user_ids` attribute populated
        from database.
    """
    group = await _usergroup_or_404(group_id, db)

    # Get all user/group links
    stm_links = select(LinkUserGroup).where(LinkUserGroup.group_id == group_id)
    res = await db.execute(stm_links)
    links = res.scalars().all()
    user_ids = [link.user_id for link in links]

    return UserGroupRead(**group.model_dump(), user_ids=user_ids)


async def _user_or_404(user_id: int, db: AsyncSession) -> UserOAuth:
    """
    Get a user from db, or raise a 404 HTTP exception if missing.

    Arguments:
        user_id: ID of the user
        db: Async db session
    """
    user = await db.get(UserOAuth, user_id, populate_existing=True)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User {user_id} not found.",
        )
    return user


async def _usergroup_or_404(usergroup_id: int, db: AsyncSession) -> UserGroup:
    user = await db.get(UserGroup, usergroup_id)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"UserGroup {usergroup_id} not found.",
        )
    return user


async def _get_default_usergroup_id(db: AsyncSession) -> int:
    stm = select(UserGroup.id).where(
        UserGroup.name == FRACTAL_DEFAULT_GROUP_NAME
    )
    res = await db.execute(stm)
    user_group_id = res.scalars().one_or_none()
    if user_group_id is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User group '{FRACTAL_DEFAULT_GROUP_NAME}' not found.",
        )
    return user_group_id


async def _verify_user_belongs_to_group(
    *, user_id: int, user_group_id: int, db: AsyncSession
):
    stm = (
        select(LinkUserGroup)
        .where(LinkUserGroup.user_id == user_id)
        .where(LinkUserGroup.group_id == user_group_id)
    )
    res = await db.execute(stm)
    link = res.scalars().one_or_none()
    if link is None:
        group = await db.get(UserGroup, user_group_id)
        if group is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"UserGroup {user_group_id} not found",
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    f"User {user_id} does not belong "
                    f"to UserGroup {user_group_id}"
                ),
            )
