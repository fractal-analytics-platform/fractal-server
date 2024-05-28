from typing import Optional

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
from ....models.v2 import DatasetV2
from ....models.v2 import JobV2
from ....models.v2 import LinkUserProjectV2
from ....models.v2 import ProjectV2
from ....models.v2 import WorkflowV2
from ....schemas.v2 import ProjectCreateV2
from ....schemas.v2 import ProjectReadV2
from ....schemas.v2 import ProjectUpdateV2
from ....security import current_active_user
from ....security import User
from ._aux_functions import _check_project_exists
from ._aux_functions import _get_project_check_owner
from ._aux_functions import _get_submitted_jobs_statement

router = APIRouter()


@router.get("/project/", response_model=list[ProjectReadV2])
async def get_list_project(
    user: User = Depends(current_active_user),
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
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[ProjectReadV2]:
    """
    Create new poject
    """

    # Check that there is no project with the same user and name
    await _check_project_exists(
        project_name=project.name, user_id=user.id, db=db
    )

    db_project = ProjectV2(**project.dict())
    db_project.user_list.append(user)

    db.add(db_project)
    await db.commit()
    await db.refresh(db_project)
    await db.close()

    return db_project


@router.get("/project/{project_id}/", response_model=ProjectReadV2)
async def read_project(
    project_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[ProjectReadV2]:
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
    user: User = Depends(current_active_user),
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

    for key, value in project_update.dict(exclude_unset=True).items():
        setattr(project, key, value)

    await db.commit()
    await db.refresh(project)
    await db.close()
    return project


@router.delete("/project/{project_id}/", status_code=204)
async def delete_project(
    project_id: int,
    user: User = Depends(current_active_user),
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

    # Cascade operations

    # Workflows
    stm = select(WorkflowV2).where(WorkflowV2.project_id == project_id)
    res = await db.execute(stm)
    workflows = res.scalars().all()
    logger.info("Start of cascade operations on Workflows.")
    for wf in workflows:
        # Cascade operations: set foreign-keys to null for jobs which are in
        # relationship with the current workflow
        stm = select(JobV2).where(JobV2.workflow_id == wf.id)
        res = await db.execute(stm)
        jobs = res.scalars().all()
        for job in jobs:
            logger.info(f"Setting Job[{job.id}].workflow_id to None.")
            job.workflow_id = None
        # Delete workflow
        logger.info(f"Adding Workflow[{wf.id}] to deletion.")
        await db.delete(wf)
    logger.info("End of cascade operations on Workflows.")

    # Dataset
    stm = select(DatasetV2).where(DatasetV2.project_id == project_id)
    res = await db.execute(stm)
    datasets = res.scalars().all()
    logger.info("Start of cascade operations on Datasets.")
    for ds in datasets:
        # Cascade operations: set foreign-keys to null for jobs which are in
        # relationship with the current dataset
        stm = select(JobV2).where(JobV2.dataset_id == ds.id)
        res = await db.execute(stm)
        jobs = res.scalars().all()
        for job in jobs:
            logger.info(f"Setting Job[{job.id}].dataset_id to None.")
            job.dataset_id = None
        # Delete dataset
        logger.info(f"Adding Dataset[{ds.id}] to deletion.")
        await db.delete(ds)
    logger.info("End of cascade operations on Datasets.")

    # Job
    logger.info("Start of cascade operations on Jobs.")
    stm = select(JobV2).where(JobV2.project_id == project_id)
    res = await db.execute(stm)
    jobs = res.scalars().all()
    for job in jobs:
        logger.info(f"Setting Job[{job.id}].project_id to None.")
        job.project_id = None
    logger.info("End of cascade operations on Jobs.")

    logger.info(f"Adding Project[{project.id}] to deletion.")
    await db.delete(project)

    logger.info("Committing changes to db...")
    await db.commit()

    logger.info("Everything  has been deleted correctly.")
    reset_logger_handlers(logger)

    return Response(status_code=status.HTTP_204_NO_CONTENT)
