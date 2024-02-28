from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Optional

from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from sqlalchemy.exc import IntegrityError
from sqlmodel import select

from .....config import get_settings
from .....logger import close_logger
from .....logger import set_logger
from .....syringe import Inject
from ....db import AsyncSession
from ....db import get_async_db
from ....models.v2 import DatasetV2
from ....models.v2 import JobV2
from ....models.v2 import LinkUserProjectV2
from ....models.v2 import ProjectV2
from ....models.v2 import WorkflowV2
from ....schemas.v2 import JobCreateV2
from ....schemas.v2 import JobReadV2
from ....schemas.v2 import JobStatusTypeV2
from ....schemas.v2 import ProjectCreateV2
from ....schemas.v2 import ProjectReadV2
from ....schemas.v2 import ProjectUpdateV2
from ....security import current_active_user
from ....security import current_active_verified_user
from ....security import User
from ._aux_functions import _check_project_exists
from ._aux_functions import _get_dataset_check_owner
from ._aux_functions import _get_project_check_owner
from ._aux_functions import _get_submitted_jobs_statement
from ._aux_functions import _get_workflow_check_owner

# from ....runner.v2 import submit_workflow # FIXME V2
# from ....runner.v2 import validate_workflow_compatibility # FIXME V2
# from ....runner.v2.common import set_start_and_last_task_index # FIXME V2

router = APIRouter()


def _encode_as_utc(dt: datetime):
    return dt.replace(tzinfo=timezone.utc).isoformat()


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
            await db.merge(job)
        await db.commit()
        # Delete workflow
        await db.delete(wf)

    # Dataset
    stm = select(DatasetV2).where(DatasetV2.project_id == project_id)
    res = await db.execute(stm)
    datasets = res.scalars().all()
    for ds in datasets:
        # Cascade operations: set foreign-keys to null for jobs which are in
        # relationship with the current dataset
        # input_dataset
        stm = select(JobV2).where(JobV2.dataset_id == ds.id)
        res = await db.execute(stm)
        jobs = res.scalars().all()
        for job in jobs:
            job.dataset_id = None
            await db.merge(job)
        await db.commit()

    # Job
    stm = select(JobV2).where(JobV2.project_id == project_id)
    res = await db.execute(stm)
    jobs = res.scalars().all()
    for job in jobs:
        job.project_id = None
        await db.merge(job)

    await db.commit()

    await db.delete(project)
    await db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    "/project/{project_id}/workflow/{workflow_id}/apply/",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=JobReadV2,
)
async def apply_workflow(
    project_id: int,
    workflow_id: int,
    apply_workflow: JobCreateV2,
    background_tasks: BackgroundTasks,
    dataset_id: int,
    user: User = Depends(current_active_verified_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[JobReadV2]:

    output = await _get_dataset_check_owner(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,
        db=db,
    )
    project = output["project"]
    dataset = output["dataset"]

    if dataset.read_only:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "Cannot apply workflow because dataset "
                f"({dataset_id=}) is read_only."
            ),
        )

    workflow = await _get_workflow_check_owner(
        project_id=project_id, workflow_id=workflow_id, user_id=user.id, db=db
    )

    if not workflow.task_list:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Workflow {workflow_id} has empty task list",
        )

    # Set values of first_task_index and last_task_index
    num_tasks = len(workflow.task_list)
    try:
        set_start_and_last_task_index = None  # ! FIXME REMOVE
        first_task_index, last_task_index = set_start_and_last_task_index(
            num_tasks,
            first_task_index=apply_workflow.first_task_index,
            last_task_index=apply_workflow.last_task_index,
        )
        apply_workflow.first_task_index = first_task_index
        apply_workflow.last_task_index = last_task_index
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "Invalid values for first_task_index or last_task_index "
                f"(with {num_tasks=}).\n"
                f"Original error: {str(e)}"
            ),
        )

    # If backend is SLURM, check that the user has required attributes
    settings = Inject(get_settings)
    backend = settings.FRACTAL_RUNNER_BACKEND
    if backend == "slurm":
        if not user.slurm_user:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    f"FRACTAL_RUNNER_BACKEND={backend}, "
                    f"but {user.slurm_user=}."
                ),
            )
        if not user.cache_dir:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    f"FRACTAL_RUNNER_BACKEND={backend}, "
                    f"but {user.cache_dir=}."
                ),
            )

    try:
        validate_workflow_compatibility = None  # ! FIXME REMOVE
        validate_workflow_compatibility(
            workflow=workflow,
            dataset=dataset,
            first_task_index=apply_workflow.first_task_index,
            last_task_index=apply_workflow.last_task_index,
        )
    except TypeError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e)
        )

    # Check that no other job with the same dataset_id is SUBMITTED
    stm = (
        select(JobV2)
        .where(JobV2.dataset_id == dataset_id)
        .where(JobV2.status == JobStatusTypeV2.SUBMITTED)
    )
    res = await db.execute(stm)
    if res.scalars().all():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Dataset {dataset_id} is already in use "
                "in submitted job(s)."
            ),
        )

    if apply_workflow.slurm_account is not None:
        if apply_workflow.slurm_account not in user.slurm_accounts:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    f"SLURM account '{apply_workflow.slurm_account}' is not "
                    "among those available to the current user"
                ),
            )
    else:
        if len(user.slurm_accounts) > 0:
            apply_workflow.slurm_account = user.slurm_accounts[0]

    # Add new ApplyWorkflow object to DB
    job = JobV2(
        project_id=project_id,
        dataset_id=dataset_id,
        workflow_id=workflow_id,
        user_email=user.email,
        input_dataset_dump=dict(
            **dataset.model_dump(
                exclude={"resource_list", "timestamp_created"}
            ),
            timestamp_created=_encode_as_utc(dataset.timestamp_created),
        ),
        workflow_dump=dict(
            **workflow.model_dump(exclude={"task_list", "timestamp_created"}),
            timestamp_created=_encode_as_utc(workflow.timestamp_created),
        ),
        project_dump=dict(
            **project.model_dump(exclude={"user_list", "timestamp_created"}),
            timestamp_created=_encode_as_utc(project.timestamp_created),
        ),
        **apply_workflow.dict(),
    )

    # Rate Limiting:
    # raise `429 TOO MANY REQUESTS` if this endpoint has been called with the
    # same database keys (Project, Workflow and Datasets) during the last
    # `settings.FRACTAL_API_SUBMIT_RATE_LIMIT` seconds.
    stm = (
        select(JobV2)
        .where(JobV2.project_id == project_id)
        .where(JobV2.workflow_id == workflow_id)
        .where(JobV2.dataset_id == dataset_id)
    )
    res = await db.execute(stm)
    db_jobs = res.scalars().all()
    if db_jobs and any(
        abs(
            job.start_timestamp
            - db_job.start_timestamp.replace(tzinfo=timezone.utc)
        )
        < timedelta(seconds=settings.FRACTAL_API_SUBMIT_RATE_LIMIT)
        for db_job in db_jobs
    ):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=(
                f"The endpoint 'POST /api/v1/project/{project_id}/workflow/"
                f"{workflow_id}/apply/' "
                "was called several times within an interval of less "
                f"than {settings.FRACTAL_API_SUBMIT_RATE_LIMIT} seconds, using"
                " the same foreign keys. If it was intentional, please wait "
                "and try again."
            ),
        )

    db.add(job)
    await db.commit()
    await db.refresh(job)

    submit_workflow = None  # ! FIXME REMOVE
    background_tasks.add_task(
        submit_workflow,
        workflow_id=workflow.id,
        dataset_id=dataset.id,
        job_id=job.id,
        worker_init=apply_workflow.worker_init,
        slurm_user=user.slurm_user,
        user_cache_dir=user.cache_dir,
    )

    await db.close()

    return job
