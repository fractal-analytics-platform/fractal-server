from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from sqlmodel import select

from .....logger import reset_logger_handlers
from .....logger import set_logger
from ....db import AsyncSession
from ....db import get_async_db
from ....models.v2 import JobV2
from ....models.v2 import LinkUserProjectV2
from ....models.v2 import ProjectV2
from ....schemas.v2 import ProjectCreateV2
from ....schemas.v2 import ProjectReadV2
from ....schemas.v2 import ProjectUpdateV2
from ._aux_functions import _check_project_exists
from ._aux_functions import _get_project_check_owner
from ._aux_functions import _get_submitted_jobs_statement
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth import current_active_user

router = APIRouter()


@router.get("/project/", response_model=list[ProjectReadV2])
async def get_list_project(
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[ProjectV2]:
    """
    Return list of projects user is member of
    """
    stm = (
        select(ProjectV2)
        .join(LinkUserProjectV2)
        .where(LinkUserProjectV2.user_id == user.id)
    )
    res = await db.execute(stm)
    project_list = res.scalars().all()
    await db.close()
    return project_list


@router.post("/project/", response_model=ProjectReadV2, status_code=201)
async def create_project(
    project: ProjectCreateV2,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> ProjectReadV2 | None:
    """
    Create new project
    """

    # Check that there is no project with the same user and name
    await _check_project_exists(
        project_name=project.name, user_id=user.id, db=db
    )

    db_project = ProjectV2(**project.model_dump())
    db_project.user_list.append(user)

    db.add(db_project)
    await db.commit()
    await db.refresh(db_project)
    await db.close()

    return db_project


@router.get("/project/{project_id}/", response_model=ProjectReadV2)
async def read_project(
    project_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> ProjectReadV2 | None:
    """
    Return info on an existing project
    """
    project = await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )
    await db.close()
    return project


@router.patch("/project/{project_id}/", response_model=ProjectReadV2)
async def update_project(
    project_id: int,
    project_update: ProjectUpdateV2,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
):
    project = await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
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
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Response:
    """
    Delete project
    """

    project = await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )
    logger = set_logger(__name__)

    # Fail if there exist jobs that are submitted and in relation with the
    # current project.
    stm = _get_submitted_jobs_statement().where(JobV2.project_id == project_id)
    res = await db.execute(stm)
    jobs = res.scalars().all()
    if jobs:
        string_ids = str([job.id for job in jobs])[1:-1]
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Cannot delete project {project.id} because it "
                f"is linked to active job(s) {string_ids}."
            ),
        )

    logger.info(f"Adding Project[{project.id}] to deletion.")
    await db.delete(project)

    logger.info("Committing changes to db...")
    await db.commit()

    logger.info("Everything  has been deleted correctly.")
    reset_logger_handlers(logger)

    return Response(status_code=status.HTTP_204_NO_CONTENT)
