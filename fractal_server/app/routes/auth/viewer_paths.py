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
    Returns a list of data paths that the user should have access to.

    In its default behavior, this endpoint returns a list made of two kinds
    of paths:

    1. All the project directories of the current user.
    2. The zarr directories of all datasets which are accessible to the current
       user (either as a project owner or as a project guest).

    NOTE: `include_shared_projects` is a legacy query-parameter name,
    which does not make a difference between owners/guests. A better
    naming would be e.g. `include_zarr_dirs`, but it would require a fix
    in `fractal-web` which is currently postponed.
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
            .distinct()
        )
        authorized_zarr_dirs: list[str] = list(res.scalars().all())
        # Note that `project_dirs` and the `authorized_zarr_dirs` may have some
        # common elements, and then the response may include non-unique items.
        return current_user.project_dirs + authorized_zarr_dirs
    else:
        return current_user.project_dirs
