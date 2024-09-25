from datetime import datetime
from datetime import timezone
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from sqlmodel import select

from .....logger import set_logger
from ....db import AsyncSession
from ....db import get_async_db
from ....models.v1 import LinkUserProject
from ....models.v1 import Project
from ....schemas.v1 import ProjectReadV1
from ._aux_functions import _get_project_check_owner
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth import current_active_user

router = APIRouter()
logger = set_logger(__name__)


def _encode_as_utc(dt: datetime):
    return dt.replace(tzinfo=timezone.utc).isoformat()


@router.get("/", response_model=list[ProjectReadV1])
async def get_list_project(
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[Project]:
    """
    Return list of projects user is member of
    """
    stm = (
        select(Project)
        .join(LinkUserProject)
        .where(LinkUserProject.user_id == user.id)
    )
    res = await db.execute(stm)
    project_list = res.scalars().all()
    await db.close()
    return project_list


@router.get("/{project_id}/", response_model=ProjectReadV1)
async def read_project(
    project_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[ProjectReadV1]:
    """
    Return info on an existing project
    """
    project = await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )
    await db.close()
    return project
