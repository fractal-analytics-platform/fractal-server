from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import ProjectV2
from fractal_server.app.routes.auth import current_active_superuser
from fractal_server.app.schemas.v2 import ProjectReadV2

router = APIRouter()


@router.get("/", response_model=list[ProjectReadV2])
async def view_project(
    id: Optional[int] = None,
    user_id: Optional[int] = None,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> list[ProjectReadV2]:
    """
    Query `ProjectV2` table.

    Args:
        id: If not `None`, select a given `project.id`.
        user_id: If not `None`, select a given `project.user_id`.
    """

    stm = select(ProjectV2)

    if id is not None:
        stm = stm.where(ProjectV2.id == id)
    if user_id is not None:
        stm = stm.where(ProjectV2.user_list.any(UserOAuth.id == user_id))

    res = await db.execute(stm)
    project_list = res.scalars().all()
    await db.close()

    return project_list
