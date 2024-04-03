from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from sqlalchemy.exc import IntegrityError
from sqlmodel import select

from .....logger import close_logger
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
    try:
        db.add(db_project)
        await db.commit()
        await db.refresh(db_project)
        await db.close()
    except IntegrityError as e:
        await db.rollback()
        logger = set_logger("create_project")
        logger.error(str(e))
        close_logger(logger)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(e),
        )

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
    for wf in workflows:
        # Cascade operations: set foreign-keys to null for jobs which are in
        # relationship with the current workflow
        stm = select(JobV2).where(JobV2.workflow_id == wf.id)
        res = await db.execute(stm)
        jobs = res.scalars().all()
        for job in jobs:
            job.workflow_id = None
        # Delete workflow
        await db.delete(wf)
    await db.commit()

    # Dataset
    stm = select(DatasetV2).where(DatasetV2.project_id == project_id)
    res = await db.execute(stm)
    datasets = res.scalars().all()
    for ds in datasets:
        # Cascade operations: set foreign-keys to null for jobs which are in
        # relationship with the current dataset
        stm = select(JobV2).where(JobV2.dataset_id == ds.id)
        res = await db.execute(stm)
        jobs = res.scalars().all()
        for job in jobs:
            job.dataset_id = None
        # Delete dataset
        await db.delete(ds)
    await db.commit()

    # Job
    stm = select(JobV2).where(JobV2.project_id == project_id)
    res = await db.execute(stm)
    jobs = res.scalars().all()
    for job in jobs:
        job.project_id = None

    await db.commit()

    await db.delete(project)
    await db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)
