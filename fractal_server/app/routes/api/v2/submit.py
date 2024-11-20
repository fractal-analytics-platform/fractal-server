import json
import os
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
from ...aux.validate_user_settings import validate_user_settings
from ._aux_functions import _get_dataset_check_owner
from ._aux_functions import _get_workflow_check_owner
from ._aux_functions import clean_app_job_list_v2
from fractal_server.app.models import TaskGroupV2
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.api.v2._aux_functions_tasks import (
    _get_task_read_access,
)
from fractal_server.app.routes.auth import current_active_verified_user


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
    user: UserOAuth = Depends(current_active_verified_user),
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
    num_tasks = len(workflow.task_list)
    if num_tasks == 0:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Workflow {workflow_id} has empty task list",
        )

    # Set values of first_task_index and last_task_index
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

    # Check that tasks have read-access and are `active`
    used_task_group_ids = set()
    for wftask in workflow.task_list[
        first_task_index : last_task_index + 1  # noqa: E203
    ]:
        task = await _get_task_read_access(
            user_id=user.id,
            task_id=wftask.task_id,
            require_active=True,
            db=db,
        )
        used_task_group_ids.add(task.taskgroupv2_id)

    # Validate user settings
    FRACTAL_RUNNER_BACKEND = settings.FRACTAL_RUNNER_BACKEND
    user_settings = await validate_user_settings(
        user=user, backend=FRACTAL_RUNNER_BACKEND, db=db
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
        if job_create.slurm_account not in user_settings.slurm_accounts:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    f"SLURM account '{job_create.slurm_account}' is not "
                    "among those available to the current user"
                ),
            )
    else:
        if len(user_settings.slurm_accounts) > 0:
            job_create.slurm_account = user_settings.slurm_accounts[0]

    # Add new Job object to DB
    job = JobV2(
        project_id=project_id,
        dataset_id=dataset_id,
        workflow_id=workflow_id,
        user_email=user.email,
        dataset_dump=json.loads(dataset.json(exclude={"images", "history"})),
        workflow_dump=json.loads(workflow.json(exclude={"task_list"})),
        project_dump=json.loads(project.json(exclude={"user_list"})),
        **job_create.dict(),
    )

    db.add(job)
    await db.commit()
    await db.refresh(job)

    # Update TaskGroupV2.timestamp_last_used
    res = await db.execute(
        select(TaskGroupV2).where(TaskGroupV2.id.in_(used_task_group_ids))
    )
    used_task_groups = res.scalars().all()
    for used_task_group in used_task_groups:
        used_task_group.timestamp_last_used = job.start_timestamp
        db.add(used_task_group)
    await db.commit()

    # Define server-side job directory
    timestamp_string = job.start_timestamp.strftime("%Y%m%d_%H%M%S")
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
            Path(user_settings.cache_dir) / f"{WORKFLOW_DIR_LOCAL.name}"
        )
    elif FRACTAL_RUNNER_BACKEND == "slurm_ssh":
        WORKFLOW_DIR_REMOTE = (
            Path(user_settings.ssh_jobs_dir) / f"{WORKFLOW_DIR_LOCAL.name}"
        )

    # Update job folders in the db
    job.working_dir = WORKFLOW_DIR_LOCAL.as_posix()
    job.working_dir_user = WORKFLOW_DIR_REMOTE.as_posix()
    await db.merge(job)
    await db.commit()

    # User appropriate FractalSSH object
    if settings.FRACTAL_RUNNER_BACKEND == "slurm_ssh":
        ssh_credentials = dict(
            user=user_settings.ssh_username,
            host=user_settings.ssh_host,
            key_path=user_settings.ssh_private_key_path,
        )
        fractal_ssh_list = request.app.state.fractal_ssh_list
        fractal_ssh = fractal_ssh_list.get(**ssh_credentials)
    else:
        fractal_ssh = None

    # Expunge user settings from db, to use in background task
    db.expunge(user_settings)

    background_tasks.add_task(
        submit_workflow,
        workflow_id=workflow.id,
        dataset_id=dataset.id,
        job_id=job.id,
        user_settings=user_settings,
        worker_init=job.worker_init,
        slurm_user=user_settings.slurm_user,
        user_cache_dir=user_settings.cache_dir,
        fractal_ssh=fractal_ssh,
    )
    request.app.state.jobsV2.append(job.id)
    logger.info(
        f"Current worker's pid is {os.getpid()}. "
        f"Current status of worker job's list "
        f"{request.app.state.jobsV2}"
    )
    await db.close()
    return job
