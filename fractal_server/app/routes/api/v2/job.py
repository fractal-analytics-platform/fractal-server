import asyncio
from collections.abc import Iterator
from pathlib import Path
from typing import Literal
from typing import Sequence

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from fastapi.params import Query
from fastapi.responses import PlainTextResponse
from fastapi.responses import StreamingResponse
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import JobV2
from fractal_server.app.models.v2 import LinkUserProjectV2
from fractal_server.app.routes.auth import get_api_guest
from fractal_server.app.routes.auth import get_api_user
from fractal_server.app.routes.aux._job import (
    _raise_422_if_status_not_submitted,
)
from fractal_server.app.routes.aux._job import _write_shutdown_file_or_422
from fractal_server.app.routes.aux._runner import _check_shutdown_is_supported
from fractal_server.app.routes.aux.validate_user_profile import (
    validate_user_profile,
)
from fractal_server.app.schemas.v2 import JobRead
from fractal_server.app.schemas.v2 import JobStatusType
from fractal_server.app.schemas.v2 import ResourceType
from fractal_server.app.schemas.v2.sharing import ProjectPermissions
from fractal_server.config import get_settings
from fractal_server.logger import set_logger
from fractal_server.runner.filenames import WORKFLOW_LOG_FILENAME
from fractal_server.ssh._fabric import SingleUseFractalSSH
from fractal_server.ssh._fabric import SSHConfig
from fractal_server.syringe import Inject
from fractal_server.utils import execute_command_sync
from fractal_server.zip_tools import _zip_folder_to_byte_stream_iterator

from ._aux_functions import _get_job_check_access
from ._aux_functions import _get_project_check_access
from ._aux_functions import _get_workflow_check_access


# https://docs.python.org/3/library/asyncio-task.html#asyncio.to_thread
# This moves the function execution to a separate thread,
# preventing it from blocking the event loop.
async def zip_folder_threaded(folder: str) -> Iterator[bytes]:
    return await asyncio.to_thread(_zip_folder_to_byte_stream_iterator, folder)


router = APIRouter()

logger = set_logger(__name__)


@router.get("/job/", response_model=list[JobRead])
async def get_user_jobs(
    user: UserOAuth = Depends(get_api_guest),
    log: bool = True,
    db: AsyncSession = Depends(get_async_db),
) -> Sequence[JobV2]:
    """
    Returns all the jobs from projects linked to the current user
    """
    stm = (
        select(JobV2)
        .join(
            LinkUserProjectV2, LinkUserProjectV2.project_id == JobV2.project_id
        )
        .where(LinkUserProjectV2.user_id == user.id)
        .where(LinkUserProjectV2.is_verified.is_(True))
        .order_by(JobV2.start_timestamp.desc())
    )
    res = await db.execute(stm)
    job_list = res.scalars().all()
    if not log:
        for job in job_list:
            setattr(job, "log", None)

    return job_list


@router.get(
    "/project/{project_id}/workflow/{workflow_id}/job/",
    response_model=list[JobRead],
)
async def get_workflow_jobs(
    project_id: int,
    workflow_id: int,
    user: UserOAuth = Depends(get_api_guest),
    db: AsyncSession = Depends(get_async_db),
) -> Sequence[JobV2]:
    """
    Returns all the jobs related to a specific workflow
    """
    await _get_workflow_check_access(
        project_id=project_id,
        workflow_id=workflow_id,
        user_id=user.id,
        required_permissions=ProjectPermissions.READ,
        db=db,
    )
    res = await db.execute(
        select(JobV2)
        .where(JobV2.workflow_id == workflow_id)
        .order_by(JobV2.start_timestamp.desc())
    )
    job_list = res.scalars().all()
    return job_list


@router.get(
    "/project/{project_id}/job/{job_id}/",
    response_model=JobRead,
)
async def read_job(
    project_id: int,
    job_id: int,
    show_tmp_logs: bool = False,
    user: UserOAuth = Depends(get_api_guest),
    db: AsyncSession = Depends(get_async_db),
) -> JobV2:
    """
    Return info on an existing job
    """

    output = await _get_job_check_access(
        project_id=project_id,
        job_id=job_id,
        user_id=user.id,
        required_permissions=ProjectPermissions.READ,
        db=db,
    )
    job = output["job"]

    if show_tmp_logs and (job.status == JobStatusType.SUBMITTED):
        try:
            with open(f"{job.working_dir}/{WORKFLOW_LOG_FILENAME}") as f:
                job.log = f.read()
        except FileNotFoundError:
            pass

    return job


@router.get(
    "/project/{project_id}/job/{job_id}/download/",
    response_class=StreamingResponse,
)
async def download_job_logs(
    project_id: int,
    job_id: int,
    user: UserOAuth = Depends(get_api_guest),
    db: AsyncSession = Depends(get_async_db),
) -> StreamingResponse:
    """
    Download zipped job folder
    """
    output = await _get_job_check_access(
        project_id=project_id,
        job_id=job_id,
        user_id=user.id,
        required_permissions=ProjectPermissions.READ,
        db=db,
    )
    job = output["job"]
    zip_name = f"{Path(job.working_dir).name}_archive.zip"

    zip_bytes_iterator = await zip_folder_threaded(job.working_dir)

    return StreamingResponse(
        zip_bytes_iterator,
        media_type="application/x-zip-compressed",
        headers={"Content-Disposition": f"attachment;filename={zip_name}"},
    )


@router.get(
    "/project/{project_id}/job/",
    response_model=list[JobRead],
)
async def get_job_list(
    project_id: int,
    user: UserOAuth = Depends(get_api_guest),
    log: bool = True,
    db: AsyncSession = Depends(get_async_db),
) -> Sequence[JobV2]:
    """
    Get job list for given project
    """
    project = await _get_project_check_access(
        project_id=project_id,
        user_id=user.id,
        required_permissions=ProjectPermissions.READ,
        db=db,
    )

    res = await db.execute(
        select(JobV2)
        .where(JobV2.project_id == project.id)
        .order_by(JobV2.start_timestamp.desc())
    )
    job_list = res.scalars().all()
    if not log:
        for job in job_list:
            setattr(job, "log", None)

    return job_list


@router.get(
    "/project/{project_id}/job/{job_id}/stop/",
    status_code=202,
)
async def stop_job(
    project_id: int,
    job_id: int,
    user: UserOAuth = Depends(get_api_user),
    db: AsyncSession = Depends(get_async_db),
) -> Response:
    """
    Stop execution of a workflow job.
    """

    _check_shutdown_is_supported()

    # Get job from DB
    output = await _get_job_check_access(
        project_id=project_id,
        job_id=job_id,
        user_id=user.id,
        required_permissions=ProjectPermissions.EXECUTE,
        db=db,
    )
    job = output["job"]

    _raise_422_if_status_not_submitted(job=job)
    _write_shutdown_file_or_422(job=job)

    return Response(status_code=status.HTTP_202_ACCEPTED)


@router.get(
    "/job/squeue/",
    response_model=str,
)
async def get_squeue(
    scope: Literal["all", "user", "accounts"] = Query(default="all"),
    user: UserOAuth = Depends(get_api_user),
    db: AsyncSession = Depends(get_async_db),
):
    settings = Inject(get_settings)
    backend = settings.FRACTAL_RUNNER_BACKEND
    if backend not in [ResourceType.SLURM_SUDO, ResourceType.SLURM_SSH]:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"This endpoint is not available for "
            f"FRACTAL_RUNNER_BACKEND={backend}.",
        )

    resource, profile = await validate_user_profile(
        user=user,
        db=db,
    )

    flags = ""
    if scope == "user":
        flags = f"--user {profile.username}"
    elif scope == "accounts" and len(user.slurm_accounts) > 0:
        flags = f"--accounts={','.join(user.slurm_accounts)}"

    command = (
        f"squeue {flags} --format="
        f'"%.12i %.9P %.24j %.14u %.14a %.11T %.12M %.6D %.4C %.10m %R"'
    )

    try:
        if resource.type == ResourceType.SLURM_SSH:
            with SingleUseFractalSSH(
                ssh_config=SSHConfig(
                    host=resource.host,
                    user=profile.username,
                    key_path=profile.ssh_key_path,
                ),
                logger_name=logger.name,
            ) as fractal_ssh:
                out = fractal_ssh.run_command(cmd=command)
                return PlainTextResponse(content=out)
        else:
            out = execute_command_sync(command=command)
            return PlainTextResponse(content=out)
    except Exception as e:
        logger.error(f"Cannot execute squeue command. Original error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Error executing squeue command - please retry later.",
        )
