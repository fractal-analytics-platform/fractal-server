from fastapi import HTTPException
from fastapi import status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from fractal_server.app.models.linkusergroup import LinkUserGroup
from fractal_server.app.models.security import UserGroup
from fractal_server.app.models.security import UserOAuth
from fractal_server.app.schemas.user import UserRead
from fractal_server.app.schemas.user_group import UserGroupRead


async def _get_single_user_with_group_names(
    user: UserOAuth,
    db: AsyncSession,
) -> UserRead:
    """
    Enrich a user object by filling its `group_names` attribute.

    Arguments:
        user: The current `UserOAuth` object
        db: Async db session

    Returns:
        A `UserRead` object with `group_names` set
    """
    stm_groups = (
        select(UserGroup)
        .join(LinkUserGroup)
        .where(LinkUserGroup.user_id == UserOAuth.id)
    )
    res = await db.execute(stm_groups)
    groups = res.scalars().unique().all()
    group_names = [group.name for group in groups]
    return UserRead(
        **user.model_dump(),
        group_names=group_names,
        oauth_accounts=user.oauth_accounts,
    )


async def _get_single_user_with_group_ids(
    user: UserOAuth,
    db: AsyncSession,
) -> UserRead:
    """
    Enrich a user object by filling its `group_ids` attribute.

    Arguments:
        user: The current `UserOAuth` object
        db: Async db session

    Returns:
        A `UserRead` object with `group_ids` set
    """
    stm_links = select(LinkUserGroup).where(LinkUserGroup.user_id == user.id)
    res = await db.execute(stm_links)
    links = res.scalars().unique().all()
    group_ids = [link.group_id for link in links]
    return UserRead(
        **user.model_dump(),
        group_ids=group_ids,
        oauth_accounts=user.oauth_accounts,
    )


async def _get_single_group_with_user_ids(
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
    # Get the UserGroup object from the database
    stm_group = select(UserGroup).where(UserGroup.id == group_id)
    res = await db.execute(stm_group)
    group = res.scalars().one_or_none()
    if group is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Group {group_id} not found.",
        )

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
            status_code=status.HTTP_404_NOT_FOUND, detail="User not found."
        )
    return user
