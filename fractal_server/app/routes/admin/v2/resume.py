"""
Dry-run endpoints for the resume of orphaned jobs (SLURM REST runner only).
"""

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Request
from fastapi import status
from fastapi.concurrency import run_in_threadpool
from pydantic.types import AwareDatetime

from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth import current_superuser_act
from fractal_server.runner.v2.resume.plan import SERVER_BOOT_TIMESTAMP
from fractal_server.runner.v2.resume.plan import ResumePlan
from fractal_server.runner.v2.resume.plan import list_orphan_candidates
from fractal_server.runner.v2.resume.plan import plan_resume

router = APIRouter()


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
