from fastapi import APIRouter
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.linkuserproject import LinkUserProjectV2
from fractal_server.app.models.v2.dataset import DatasetV2
from fractal_server.app.models.v2.project import ProjectV2
from fractal_server.app.routes.auth import current_user_act_ver

router_viewer_paths = APIRouter()


@router_viewer_paths.get(
    "/current-user/allowed-viewer-paths/", response_model=list[str]
)
async def get_current_user_allowed_viewer_paths(
    include_shared_projects: bool = True,
    current_user: UserOAuth = Depends(current_user_act_ver),
    db: AsyncSession = Depends(get_async_db),
) -> list[str]:
    """
    Returns the allowed viewer paths for current user.
    """
    authorized_paths = current_user.project_dirs.copy()

    if include_shared_projects:
        res = await db.execute(
            select(DatasetV2.zarr_dir)
            .join(ProjectV2, ProjectV2.id == DatasetV2.project_id)
            .join(
                LinkUserProjectV2, LinkUserProjectV2.project_id == ProjectV2.id
            )
            .where(LinkUserProjectV2.user_id == current_user.id)
            .where(LinkUserProjectV2.is_owner.is_(False))
        )
        authorized_paths.extend(res.unique().scalars().all())
        # Note that `project_dirs` and the `db.execute` result may have some
        # common elements, and then this list may have non-unique items.

    return authorized_paths
