import json
import os
from pathlib import Path

from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Request
from fastapi import status
from sqlmodel import select
from sqlmodel import update

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import Profile
from fractal_server.app.models import TaskGroupV2
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import JobV2
from fractal_server.app.routes.api.v2._aux_functions_tasks import (
    _get_task_read_access,
)
from fractal_server.app.routes.auth import current_user_act_ver_prof
from fractal_server.app.routes.aux.validate_user_profile import (
    validate_user_profile,
)
from fractal_server.app.schemas.v2 import JobCreate
from fractal_server.app.schemas.v2 import JobRead
from fractal_server.app.schemas.v2 import JobStatusType
from fractal_server.app.schemas.v2 import ResourceType
from fractal_server.app.schemas.v2.sharing import ProjectPermissions
from fractal_server.config import get_settings
from fractal_server.logger import set_logger
from fractal_server.runner.set_start_and_last_task_index import (
    set_start_and_last_task_index,
)
from fractal_server.runner.v2.submit_workflow import submit_workflow
from fractal_server.syringe import Inject

from ._aux_functions import _get_dataset_check_access
from ._aux_functions import _get_workflow_check_access
from ._aux_functions import clean_app_job_list
from ._aux_functions_tasks import _check_type_filters_compatibility

FRACTAL_CACHE_DIR = ".fractal_cache"
router = APIRouter()
logger = set_logger(__name__)


@router.post(
    "/project/{project_id}/job/submit/",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=JobRead,
)
async def submit_job(
    project_id: int,
    workflow_id: int,
    dataset_id: int,
    job_create: JobCreate,
    background_tasks: BackgroundTasks,
    request: Request,
    user: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
) -> JobRead | None:
    # Remove non-submitted Jobs from the app state when the list grows
    # beyond a threshold
    # NOTE: this may lead to a race condition on `app.state.jobs` if two
    # requests take place at the same time and `clean_app_job_list` is
    # somewhat slow.
    settings = Inject(get_settings)
    if len(request.app.state.jobs) > settings.FRACTAL_API_MAX_JOB_LIST_LENGTH:
        new_jobs_list = await clean_app_job_list(db, request.app.state.jobs)
        request.app.state.jobs = new_jobs_list

    output = await _get_dataset_check_access(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,
        required_permissions=ProjectPermissions.EXECUTE,
        db=db,
    )
    project = output["project"]
    dataset = output["dataset"]

    # Verify that user's resource matches with project resource
    res = await db.execute(
        select(Profile.resource_id).where(Profile.id == user.profile_id)
    )
    user_resource_id = res.scalar_one()
    if project.resource_id != user_resource_id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Project resource does not match with user's resource",
        )

    workflow = await _get_workflow_check_access(
        project_id=project_id,
        workflow_id=workflow_id,
        user_id=user.id,
        required_permissions=ProjectPermissions.EXECUTE,
        db=db,
    )
    num_tasks = len(workflow.task_list)
    if num_tasks == 0:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
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
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                "Invalid values for first_task_index or last_task_index "
                f"(with {num_tasks=}).\n"
                f"Original error: {str(e)}"
            ),
        )

    # Check that tasks have read-access and are `active`
    used_task_group_ids = set()
    for wftask in workflow.task_list[first_task_index : last_task_index + 1]:
        task = await _get_task_read_access(
            user_id=user.id,
            task_id=wftask.task_id,
            require_active=True,
            db=db,
        )
        _check_type_filters_compatibility(
            task_input_types=task.input_types,
            wftask_type_filters=wftask.type_filters,
        )
        used_task_group_ids.add(task.taskgroupv2_id)

    # Get validated resource and profile
    resource, profile = await validate_user_profile(
        user=user,
        db=db,
    )
    if resource.prevent_new_submissions:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"The '{resource.name}' resource does not currently accept "
                "new job submissions."
            ),
        )

    # User appropriate FractalSSH object
    if resource.type == ResourceType.SLURM_SSH:
        ssh_config = dict(
            user=profile.username,
            host=resource.host,
            key_path=profile.ssh_key_path,
        )
        fractal_ssh_list = request.app.state.fractal_ssh_list
        try:
            fractal_ssh = fractal_ssh_list.get(**ssh_config)
        except Exception as e:
            logger.error(
                "Could not get a valid SSH connection in the submit endpoint. "
                f"Original error: '{str(e)}'."
            )
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail="Error in setting up the SSH connection.",
            )
    else:
        fractal_ssh = None

    # Assign `job_create.slurm_account`
    if job_create.slurm_account is not None:
        if job_create.slurm_account not in user.slurm_accounts:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail=(
                    f"SLURM account '{job_create.slurm_account}' is not "
                    "among those available to the current user"
                ),
            )
    else:
        if len(user.slurm_accounts) > 0:
            job_create.slurm_account = user.slurm_accounts[0]

    # Check that no other job with the same dataset_id is SUBMITTED
    stm = (
        select(JobV2)
        .where(JobV2.dataset_id == dataset_id)
        .where(JobV2.status == JobStatusType.SUBMITTED)
    )
    res = await db.execute(stm)
    if res.scalars().all():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"Dataset {dataset_id} is already in use in submitted job(s)."
            ),
        )

    # Add new Job object to DB
    job = JobV2(
        project_id=project_id,
        dataset_id=dataset_id,
        workflow_id=workflow_id,
        user_email=user.email,
        dataset_dump=json.loads(
            dataset.model_dump_json(exclude={"images", "history"})
        ),
        workflow_dump=json.loads(
            workflow.model_dump_json(exclude={"task_list"})
        ),
        project_dump=json.loads(
            project.model_dump_json(exclude={"resource_id"})
        ),
        **job_create.model_dump(),
    )

    db.add(job)
    await db.commit()
    await db.refresh(job)

    # Update TaskGroupV2.timestamp_last_used
    await db.execute(
        update(TaskGroupV2)
        .where(TaskGroupV2.id.in_(used_task_group_ids))
        .values(timestamp_last_used=job.start_timestamp)
    )
    await db.commit()

    # Define `cache_dir`
    cache_dir = Path(user.project_dirs[0], FRACTAL_CACHE_DIR)

    # Define server-side and user-side job directories
    timestamp_string = job.start_timestamp.strftime(r"%Y%m%d_%H%M%S")
    working_dir = Path(resource.jobs_local_dir) / (
        f"proj_v2_{project_id:07d}_wf_{workflow_id:07d}_job_{job.id:07d}"
        f"_{timestamp_string}"
    )
    match resource.type:
        case ResourceType.LOCAL:
            working_dir_user = working_dir
        case ResourceType.SLURM_SUDO:
            working_dir_user = cache_dir / working_dir.name
        case ResourceType.SLURM_SSH:
            working_dir_user = Path(profile.jobs_remote_dir, working_dir.name)
    job.working_dir = working_dir.as_posix()
    job.working_dir_user = working_dir_user.as_posix()
    await db.merge(job)
    await db.commit()

    background_tasks.add_task(
        submit_workflow,
        workflow_id=workflow.id,
        dataset_id=dataset.id,
        job_id=job.id,
        user_id=user.id,
        worker_init=job.worker_init,
        user_cache_dir=cache_dir.as_posix(),
        fractal_ssh=fractal_ssh,
        resource=resource,
        profile=profile,
    )
    request.app.state.jobs.append(job.id)
    logger.info(
        f"Job {job.id}, worker with pid {os.getpid()}. "
        f"Worker jobs list: {request.app.state.jobs}."
    )
    return job
