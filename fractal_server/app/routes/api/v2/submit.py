import os
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Optional

from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Request
from fastapi import status
from sqlmodel import select

from .....config import get_settings
from .....logger import set_logger
from .....syringe import Inject
from .....utils import get_timestamp
from ....db import AsyncSession
from ....db import get_async_db
from ....models.v2 import JobV2
from ....runner.set_start_and_last_task_index import (
    set_start_and_last_task_index,
)
from ....runner.v2 import submit_workflow
from ....schemas.v2 import JobCreateV2
from ....schemas.v2 import JobReadV2
from ....schemas.v2 import JobStatusTypeV2
from ....security import current_active_verified_user
from ....security import User
from ._aux_functions import _get_dataset_check_owner
from ._aux_functions import _get_workflow_check_owner
from ._aux_functions import clean_app_job_list_v2


def _encode_as_utc(dt: datetime):
    return dt.replace(tzinfo=timezone.utc).isoformat()


router = APIRouter()
logger = set_logger(__name__)


@router.post(
    "/project/{project_id}/job/submit/",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=JobReadV2,
)
async def apply_workflow(
    project_id: int,
    workflow_id: int,
    dataset_id: int,
    job_create: JobCreateV2,
    background_tasks: BackgroundTasks,
    request: Request,
    user: User = Depends(current_active_verified_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[JobReadV2]:

    # Remove non-submitted V2 jobs from the app state when the list grows
    # beyond a threshold
    settings = Inject(get_settings)
    if (
        len(request.app.state.jobsV2)
        > settings.FRACTAL_API_MAX_JOB_LIST_LENGTH
    ):
        new_jobs_list = await clean_app_job_list_v2(
            db, request.app.state.jobsV2
        )
        request.app.state.jobsV2 = new_jobs_list

    output = await _get_dataset_check_owner(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,
        db=db,
    )
    project = output["project"]
    dataset = output["dataset"]

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
        first_task_index, last_task_index = set_start_and_last_task_index(
            num_tasks,
            first_task_index=job_create.first_task_index,
            last_task_index=job_create.last_task_index,
        )
        job_create.first_task_index = first_task_index
        job_create.last_task_index = last_task_index
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
    FRACTAL_RUNNER_BACKEND = settings.FRACTAL_RUNNER_BACKEND
    if FRACTAL_RUNNER_BACKEND == "slurm":
        if not user.slurm_user:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"{FRACTAL_RUNNER_BACKEND=}, but {user.slurm_user=}.",
            )
        if not user.cache_dir:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"{FRACTAL_RUNNER_BACKEND=}, but {user.cache_dir=}.",
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

    if job_create.slurm_account is not None:
        if job_create.slurm_account not in user.slurm_accounts:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    f"SLURM account '{job_create.slurm_account}' is not "
                    "among those available to the current user"
                ),
            )
    else:
        if len(user.slurm_accounts) > 0:
            job_create.slurm_account = user.slurm_accounts[0]

    # Add new Job object to DB
    job = JobV2(
        project_id=project_id,
        dataset_id=dataset_id,
        workflow_id=workflow_id,
        user_email=user.email,
        dataset_dump=dict(
            **dataset.model_dump(
                exclude={"images", "history", "timestamp_created"}
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
        **job_create.dict(),
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
                f"The endpoint 'POST /api/v2/project/{project_id}/job/submit/'"
                " was called several times within an interval of less "
                f"than {settings.FRACTAL_API_SUBMIT_RATE_LIMIT} seconds, using"
                " the same foreign keys. If it was intentional, please wait "
                "and try again."
            ),
        )

    db.add(job)
    await db.commit()
    await db.refresh(job)

    # Define server-side job directory
    timestamp_string = get_timestamp().strftime("%Y%m%d_%H%M%S")
    WORKFLOW_DIR_LOCAL = settings.FRACTAL_RUNNER_WORKING_BASE_DIR / (
        f"proj_v2_{project_id:07d}_wf_{workflow_id:07d}_job_{job.id:07d}"
        f"_{timestamp_string}"
    )

    # Define user-side job directory
    if FRACTAL_RUNNER_BACKEND == "local":
        WORKFLOW_DIR_REMOTE = WORKFLOW_DIR_LOCAL
    elif FRACTAL_RUNNER_BACKEND == "local_experimental":
        WORKFLOW_DIR_REMOTE = WORKFLOW_DIR_LOCAL
    elif FRACTAL_RUNNER_BACKEND == "slurm":
        WORKFLOW_DIR_REMOTE = (
            Path(user.cache_dir) / f"{WORKFLOW_DIR_LOCAL.name}"
        )
    elif FRACTAL_RUNNER_BACKEND == "slurm_ssh":
        WORKFLOW_DIR_REMOTE = (
            Path(settings.FRACTAL_SLURM_SSH_WORKING_BASE_DIR)
            / f"{WORKFLOW_DIR_LOCAL.name}"
        )

    # Update job folders in the db
    job.working_dir = WORKFLOW_DIR_LOCAL.as_posix()
    job.working_dir_user = WORKFLOW_DIR_REMOTE.as_posix()
    await db.merge(job)
    await db.commit()

    background_tasks.add_task(
        submit_workflow,
        workflow_id=workflow.id,
        dataset_id=dataset.id,
        job_id=job.id,
        worker_init=job.worker_init,
        slurm_user=user.slurm_user,
        user_cache_dir=user.cache_dir,
        connection=request.app.state.connection,
    )
    request.app.state.jobsV2.append(job.id)
    logger.info(
        f"Current worker's pid is {os.getpid()}. "
        f"Current status of worker job's list "
        f"{request.app.state.jobsV2}"
    )
    await db.close()
    return job
