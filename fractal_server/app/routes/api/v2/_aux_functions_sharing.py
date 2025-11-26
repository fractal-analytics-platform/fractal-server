from fastapi import HTTPException
from fastapi import status
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import LinkUserProjectV2


async def raise_403_if_not_owner(
    *,
    user_id: int,
    project_id: int,
    db: AsyncSession,
) -> None:
    """
    Raises 403 if User[`user_id`] is not owner of Project[`project_id`],
    regardless of whether the User or Project exists.
    """
    res = await db.execute(
        select(LinkUserProjectV2)
        .where(LinkUserProjectV2.project_id == project_id)
        .where(LinkUserProjectV2.user_id == user_id)
        .where(LinkUserProjectV2.is_owner.is_(True))
    )
    link = res.scalars().one_or_none()
    if link is None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Current user is not the project owner.",
        )
    return link


async def get_link_or_404(
    *, user_id: int, project_id: int, db: AsyncSession
) -> LinkUserProjectV2:
    """
    Raises 404 if User[`user_id`] is not linked to Project[`project_id`],
    regardless of whether the User or Project exists.
    """
    link = await db.get(LinkUserProjectV2, (project_id, user_id))
    if link is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User is not linked to project.",
        )
    return link


async def get_pending_invitation_or_404(
    *, user_id: int, project_id: int, db: AsyncSession
) -> LinkUserProjectV2:
    """
    Raises 404 if User[`user_id`] has not a pending invitation to
    Project[`project_id`], regardless of whether the User or Project exists.
    """
    link = await get_link_or_404(user_id=user_id, project_id=project_id, db=db)
    if link.is_verified:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No pending invitation for user on this project.",
        )
    return link


async def raise_422_if_link_exists(
    *, user_id: int, project_id: int, db: AsyncSession
) -> None:
    """
    Raises 422 if User[`user_id`] is linked Project[`project_id`], regardless
    of whether the User or Project exists.
    """
    link = await db.get(LinkUserProjectV2, (project_id, user_id))
    if link is not None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="User is already associated to project.",
        )
    return


async def get_user_id_from_email_or_404(
    *, user_email: str, db: AsyncSession
) -> int:
    """
    Raises 404 if there is no User with email `user_email`.
    """
    res = await db.execute(
        select(UserOAuth.id).where(UserOAuth.email == user_email)
    )
    user_id = res.scalar_one_or_none()
    if user_id is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User not found."
        )
    return user_id
