"""
Definition `/auth/group-names/` endpoints
"""
from fastapi import APIRouter
from fastapi import Depends
from fastapi import status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from . import current_active_user
from ...db import get_async_db
from fractal_server.app.models import UserGroup
from fractal_server.app.models import UserOAuth

router_group_names = APIRouter()


@router_group_names.get(
    "/group-names/", response_model=list[str], status_code=status.HTTP_200_OK
)
async def get_list_user_group_names(
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[str]:
    """
    Return the available group names.

    This endpoint is not restricted to superusers.
    """
    stm_all_groups = select(UserGroup)
    res = await db.execute(stm_all_groups)
    groups = res.scalars().all()
    group_names = [group.name for group in groups]
    return group_names
