from fastapi import HTTPException
from fastapi import status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from ...models.linkusergroup import LinkUserGroup
from ...models.security import UserGroup
from ...schemas.user_group import UserGroupRead


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
