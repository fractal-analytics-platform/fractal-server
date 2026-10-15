"""
Endpoints for the resume of orphaned jobs (SLURM REST runner only), and the
optional resume at server startup.
"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import Depends
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Request
from fastapi import status
from fastapi.concurrency import run_in_threadpool
from pydantic.types import AwareDatetime
from sqlalchemy import Connection
from sqlalchemy import text

from fractal_server.app.db import DB
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth import current_superuser_act
from fractal_server.logger import set_logger
from fractal_server.runner.executors.slurm_rest.settings import (
    get_slurm_rest_settings,
)
from fractal_server.runner.v2.resume.actions import ResumeError
from fractal_server.runner.v2.resume.actions import ResumeLaunch
from fractal_server.runner.v2.resume.actions import apply_resume_plan
from fractal_server.runner.v2.resume.plan import BOOT_MARKER
from fractal_server.runner.v2.resume.plan import SERVER_BOOT_TIMESTAMP
from fractal_server.runner.v2.resume.plan import ResumeAction
from fractal_server.runner.v2.resume.plan import ResumePlan
from fractal_server.runner.v2.resume.plan import list_orphan_candidates
from fractal_server.runner.v2.resume.plan import plan_resume
from fractal_server.runner.v2.submit_workflow import submit_workflow

logger = set_logger(__name__)
# Strong references, so that running tasks are not garbage-collected
_startup_tasks: set[asyncio.Task] = set()
# Postgres advisory-lock key, unused elsewhere in fractal-server
STARTUP_LOCK_KEY = 5_065_183_418_093_015_381


def _submit_workflow_kwargs(
    app: FastAPI, launch: ResumeLaunch
) -> dict[str, Any]:
    fractal_ssh = app.state.fractal_ssh_list.get(
        host=launch.resource.host,
        user=launch.profile.username,
        key_path=launch.profile.ssh_key_path,
    )
    return dict(
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


def _keep_task(task: asyncio.Task) -> None:
    _startup_tasks.add(task)
    task.add_done_callback(_startup_tasks.discard)


def _try_startup_lock() -> Connection | None:
    """
    Take the startup-resume lock on a dedicated connection, without waiting.
    """
    connection = DB.engine_sync().connect()
    acquired = connection.execute(
        text("SELECT pg_try_advisory_lock(:key)"),
        dict(key=STARTUP_LOCK_KEY),
    ).scalar()
    connection.commit()
    if acquired:
        return connection
    connection.close()
    return None


def _release_startup_lock(connection: Connection) -> None:
    # Closing the DBAPI connection releases the lock, also if a query failed
    connection.invalidate()
    connection.close()


async def resume_orphans_on_startup(app: FastAPI) -> None:
    """
    Apply the resume plan of every job started before the server start.

    Every gunicorn worker calls this; the advisory lock lets one of them
    run it. Errors are logged, never raised.
    """
    try:
        lock = await run_in_threadpool(_try_startup_lock)
    except Exception as e:
        logger.error(f"Startup resume: cannot take the lock: {e}")
        return
    if lock is None:
        logger.info("Startup resume: run by another worker.")
        return
    try:
        await _resume_orphans(app)
    finally:
        _release_startup_lock(lock)


async def _resume_orphans(app: FastAPI) -> None:
    try:
        job_ids = await run_in_threadpool(
            list_orphan_candidates,
            started_before=SERVER_BOOT_TIMESTAMP,
            exclude_marker=BOOT_MARKER,
        )
    except Exception as e:
        logger.error(f"Startup resume: cannot list orphan jobs: {e}")
        return
    logger.info(f"Startup resume: candidate jobs {job_ids}.")
    for job_id in job_ids:
        try:
            plan = await run_in_threadpool(
                plan_resume,
                job_id=job_id,
                fractal_ssh_list=app.state.fractal_ssh_list,
                started_before=SERVER_BOOT_TIMESTAMP,
            )
            if plan is None or plan.action == ResumeAction.NONE:
                continue
            if plan.action == ResumeAction.REFUSE:
                logger.warning(
                    f"Startup resume: job {job_id} refused: {plan.reasons}"
                )
                continue
            launch = await run_in_threadpool(apply_resume_plan, plan)
        except ResumeError as e:
            logger.info(f"Startup resume: job {job_id} skipped: {e}")
            continue
        except Exception as e:
            logger.error(f"Startup resume: job {job_id} failed: {e}")
            continue
        logger.info(
            f"Startup resume: job {job_id}: {plan.case} -> {plan.action}."
        )
        if launch is not None:
            kwargs = _submit_workflow_kwargs(app, launch)
            task = asyncio.create_task(
                run_in_threadpool(submit_workflow, **kwargs)
            )
            _keep_task(task)
            app.state.jobs.append(job_id)


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    # Runs after the app lifespan has set `app.state`
    task = None
    if (
        get_slurm_rest_settings().FRACTAL_RESUME_ON_STARTUP == "true"
        and app.state.fractal_ssh_list is not None
    ):
        task = asyncio.create_task(resume_orphans_on_startup(app))
        _keep_task(task)
    yield
    # Not awaited: a pending database or SSH call would delay the shutdown
    if task is not None:
        task.cancel()


router = APIRouter(lifespan=_lifespan)


# --- Endpoints for managing orphaned job resumes ---


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
        started_before: Cutoff; defaults to the server start.
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
            Also accept jobs started after the server start; only use it
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
        background_tasks.add_task(
            submit_workflow, **_submit_workflow_kwargs(request.app, launch)
        )
        request.app.state.jobs.append(job_id)
    return plan
