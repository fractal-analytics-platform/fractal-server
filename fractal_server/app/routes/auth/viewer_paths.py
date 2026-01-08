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

    NOTE: `include_shared_projects` is an obsolete query-parameter name,
    because it does not make a difference between owners/guests. A better
    naming would be e.g. `include_zarr_dirs`, but it would require a fix
    in `fractal-web` as well.
    """
    if include_shared_projects:
        res = await db.execute(
            select(DatasetV2.zarr_dir)
            .join(ProjectV2, ProjectV2.id == DatasetV2.project_id)
            .join(
                LinkUserProjectV2, LinkUserProjectV2.project_id == ProjectV2.id
            )
            .where(LinkUserProjectV2.user_id == current_user.id)
            .where(LinkUserProjectV2.is_verified.is_(True))
        )
        authorized_zarr_dirs = list(res.unique().scalars().all())
        # Note that `project_dirs` and the `authorized_zarr_dirs` may have some
        # common elements, and then the response may include non-unique items.
        return current_user.project_dirs + authorized_zarr_dirs
    else:
        return current_user.project_dirs
