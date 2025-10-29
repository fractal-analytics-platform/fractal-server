from fastapi import HTTPException
from fastapi import status

from fractal_server.app.db import AsyncSession
from fractal_server.app.models import UserGroup


async def verify_resource_and_user_group_match(
    *,
    user_group_id: int | None,
    db: AsyncSession,
    task_group_resource_id: int,
) -> None:
    """
    FIXME
    """
    if user_group_id is not None:
        user_group = await db.get(UserGroup, user_group_id)
        if user_group.resource_id != task_group_resource_id:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail="FIXME",
            )
