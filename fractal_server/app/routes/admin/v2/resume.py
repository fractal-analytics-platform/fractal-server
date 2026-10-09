"""
Endpoints for the resume of orphaned jobs (SLURM REST runner only).
"""

from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Request
from fastapi import status
from fastapi.concurrency import run_in_threadpool
from pydantic.types import AwareDatetime

from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth import current_superuser_act
from fractal_server.logger import set_logger
from fractal_server.runner.v2.resume.actions import ResumeError
from fractal_server.runner.v2.resume.actions import apply_resume_plan
from fractal_server.runner.v2.resume.plan import SERVER_BOOT_TIMESTAMP
from fractal_server.runner.v2.resume.plan import ResumePlan
from fractal_server.runner.v2.resume.plan import list_orphan_candidates
from fractal_server.runner.v2.resume.plan import plan_resume
from fractal_server.runner.v2.submit_workflow import submit_workflow

router = APIRouter()
logger = set_logger(__name__)


@router.get("/orphans/", response_model=list[ResumePlan])
async def get_orphan_resume_plans(
    request: Request,
    started_before: AwareDatetime | None = None,
    check_remote: bool = True,
    superuser: UserOAuth = Depends(current_superuser_act),
) -> list[ResumePlan]:
    """
    Plan the resume of all `submitted` jobs started before a cutoff.

    Args:
        started_before: Cutoff; defaults to the start of this server process.
        check_remote: Whether to query SLURM and read remote files.
    """
    cutoff = started_before or SERVER_BOOT_TIMESTAMP
    job_ids = await run_in_threadpool(
        list_orphan_candidates, started_before=cutoff
    )
    plans = []
    for job_id in job_ids:
        plan = await run_in_threadpool(
            plan_resume,
            job_id=job_id,
            fractal_ssh_list=request.app.state.fractal_ssh_list,
            started_before=cutoff,
            check_remote=check_remote,
        )
        if plan is not None:
            plans.append(plan)
    return plans


@router.get("/job/{job_id}/", response_model=ResumePlan)
async def get_job_resume_plan(
    job_id: int,
    request: Request,
    check_remote: bool = True,
    superuser: UserOAuth = Depends(current_superuser_act),
) -> ResumePlan:
    """
    Plan the resume of a single job, without the start-time cutoff.
    """
    plan = await run_in_threadpool(
        plan_resume,
        job_id=job_id,
        fractal_ssh_list=request.app.state.fractal_ssh_list,
        check_remote=check_remote,
    )
    if plan is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found.",
        )
    return plan


@router.post("/job/{job_id}/", response_model=ResumePlan)
async def resume_job(
    job_id: int,
    request: Request,
    background_tasks: BackgroundTasks,
    force: bool = False,
    superuser: UserOAuth = Depends(current_superuser_act),
) -> ResumePlan:
    """
    Resume an orphaned job, or mark it done/failed, following its plan.

    Args:
        force:
            Also accept jobs started after this server process; only use it
            if the process that ran the job is gone.
    """
    if job_id in request.app.state.jobs:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"Job {job_id} is running in this server process.",
        )
    fractal_ssh_list = request.app.state.fractal_ssh_list
    plan = await run_in_threadpool(
        plan_resume,
        job_id=job_id,
        fractal_ssh_list=fractal_ssh_list,
        started_before=None if force else SERVER_BOOT_TIMESTAMP,
    )
    if plan is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found.",
        )
    try:
        launch = await run_in_threadpool(apply_resume_plan, plan)
    except ResumeError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=dict(error=str(e), plan=plan.model_dump(mode="json")),
        )
    logger.info(f"Job {job_id}: {plan.case} -> {plan.action}.")
    if launch is not None:
        fractal_ssh = fractal_ssh_list.get(
            host=launch.resource.host,
            user=launch.profile.username,
            key_path=launch.profile.ssh_key_path,
        )
        background_tasks.add_task(
            submit_workflow,
            workflow_id=launch.workflow_id,
            dataset_id=launch.dataset_id,
            job_id=launch.job_id,
            user_id=launch.user_id,
            worker_init=launch.worker_init,
            user_cache_dir=launch.user_cache_dir,
            resource=launch.resource,
            profile=launch.profile,
            fractal_ssh=fractal_ssh,
            resume=launch.resume,
        )
        request.app.state.jobs.append(job_id)
    return plan
