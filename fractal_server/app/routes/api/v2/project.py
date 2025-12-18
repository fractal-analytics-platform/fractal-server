from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import JobV2
from fractal_server.app.models.v2 import LinkUserProjectV2
from fractal_server.app.models.v2 import ProjectV2
from fractal_server.app.routes.auth import current_user_act_ver_prof
from fractal_server.app.routes.aux.validate_user_profile import (
    validate_user_profile,
)
from fractal_server.app.schemas.v2 import ProjectCreate
from fractal_server.app.schemas.v2 import ProjectPermissions
from fractal_server.app.schemas.v2 import ProjectRead
from fractal_server.app.schemas.v2 import ProjectUpdate
from fractal_server.logger import set_logger

from ._aux_functions import _check_project_exists
from ._aux_functions import _get_project_check_access
from ._aux_functions import _get_submitted_jobs_statement

logger = set_logger(__name__)
router = APIRouter()


@router.get("/project/", response_model=list[ProjectRead])
async def get_list_project(
    is_owner: bool = True,
    user: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
) -> list[ProjectV2]:
    """
    Return list of projects user is member of
    """
    stm = (
        select(ProjectV2)
        .join(LinkUserProjectV2, LinkUserProjectV2.project_id == ProjectV2.id)
        .where(LinkUserProjectV2.user_id == user.id)
        .where(LinkUserProjectV2.is_owner == is_owner)
        .where(LinkUserProjectV2.is_verified.is_(True))
    )
    res = await db.execute(stm)
    project_list = res.scalars().all()
    await db.close()
    return project_list


@router.post("/project/", response_model=ProjectRead, status_code=201)
async def create_project(
    project: ProjectCreate,
    user: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
) -> ProjectRead | None:
    """
    Create new project
    """

    # Get validated resource and profile
    resource, profile = await validate_user_profile(
        user=user,
        db=db,
    )
    resource_id = resource.id

    # Check that there is no project with the same user and name
    await _check_project_exists(
        project_name=project.name, user_id=user.id, db=db
    )

    db_project = ProjectV2(**project.model_dump(), resource_id=resource_id)
    db.add(db_project)
    await db.flush()

    link = LinkUserProjectV2(
        project_id=db_project.id,
        user_id=user.id,
        is_owner=True,
        is_verified=True,
        permissions=ProjectPermissions.EXECUTE,
    )
    db.add(link)

    await db.commit()
    await db.refresh(db_project)

    return db_project


@router.get("/project/{project_id}/", response_model=ProjectRead)
async def read_project(
    project_id: int,
    user: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
) -> ProjectRead | None:
    """
    Return info on an existing project
    """
    project = await _get_project_check_access(
        project_id=project_id,
        user_id=user.id,
        required_permissions=ProjectPermissions.READ,
        db=db,
    )
    await db.close()
    return project


@router.patch("/project/{project_id}/", response_model=ProjectRead)
async def update_project(
    project_id: int,
    project_update: ProjectUpdate,
    user: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
):
    project = await _get_project_check_access(
        project_id=project_id,
        user_id=user.id,
        required_permissions=ProjectPermissions.WRITE,
        db=db,
    )

    # Check that there is no project with the same user and name
    if project_update.name is not None:
        await _check_project_exists(
            project_name=project_update.name, user_id=user.id, db=db
        )

    for key, value in project_update.model_dump(exclude_unset=True).items():
        setattr(project, key, value)

    await db.commit()
    await db.refresh(project)
    await db.close()
    return project


@router.delete("/project/{project_id}/", status_code=204)
async def delete_project(
    project_id: int,
    user: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
) -> Response:
    """
    Delete project
    """

    project = await _get_project_check_access(
        project_id=project_id,
        user_id=user.id,
        required_permissions=ProjectPermissions.EXECUTE,
        db=db,
    )
    link_user_project = await db.get(LinkUserProjectV2, (project_id, user.id))
    if not link_user_project.is_owner:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only the owner can delete a Project.",
        )

    # Fail if there exist jobs that are submitted and in relation with the
    # current project.
    stm = _get_submitted_jobs_statement().where(JobV2.project_id == project_id)
    res = await db.execute(stm)
    jobs = res.scalars().all()
    if jobs:
        string_ids = str([job.id for job in jobs])[1:-1]
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"Cannot delete project {project.id} because it "
                f"is linked to active job(s) {string_ids}."
            ),
        )

    logger.debug(f"Add project {project.id} to deletion.")
    await db.delete(project)

    logger.debug("Commit changes to db")
    await db.commit()

    logger.debug("Everything  has been deleted correctly.")

    return Response(status_code=status.HTTP_204_NO_CONTENT)
