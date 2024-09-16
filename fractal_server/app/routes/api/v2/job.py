from pathlib import Path
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from fastapi import Response
from fastapi import status
from fastapi.responses import StreamingResponse
from sqlmodel import select

from .....zip_tools import _zip_folder_to_byte_stream_iterator
from ....db import AsyncSession
from ....db import get_async_db
from ....models.v2 import JobV2
from ....models.v2 import ProjectV2
from ....runner.filenames import WORKFLOW_LOG_FILENAME
from ....schemas.v2 import JobReadV2
from ....schemas.v2 import JobStatusTypeV2
from ...aux._job import _write_shutdown_file
from ...aux._runner import _check_shutdown_is_supported
from ._aux_functions import _get_job_check_owner
from ._aux_functions import _get_project_check_owner
from ._aux_functions import _get_workflow_check_owner
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth import current_active_user

router = APIRouter()


@router.get("/job/", response_model=list[JobReadV2])
async def get_user_jobs(
    user: UserOAuth = Depends(current_active_user),
    log: bool = True,
    db: AsyncSession = Depends(get_async_db),
) -> list[JobReadV2]:
    """
    Returns all the jobs of the current user
    """
    stm = (
        select(JobV2)
        .join(ProjectV2)
        .where(ProjectV2.user_list.any(UserOAuth.id == user.id))
    )
    res = await db.execute(stm)
    job_list = res.scalars().all()
    await db.close()
    if not log:
        for job in job_list:
            setattr(job, "log", None)

    return job_list


@router.get(
    "/project/{project_id}/workflow/{workflow_id}/job/",
    response_model=list[JobReadV2],
)
async def get_workflow_jobs(
    project_id: int,
    workflow_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[list[JobReadV2]]:
    """
    Returns all the jobs related to a specific workflow
    """
    await _get_workflow_check_owner(
        project_id=project_id, workflow_id=workflow_id, user_id=user.id, db=db
    )
    stm = select(JobV2).where(JobV2.workflow_id == workflow_id)
    res = await db.execute(stm)
    job_list = res.scalars().all()
    return job_list


@router.get(
    "/project/{project_id}/job/{job_id}/",
    response_model=JobReadV2,
)
async def read_job(
    project_id: int,
    job_id: int,
    show_tmp_logs: bool = False,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[JobReadV2]:
    """
    Return info on an existing job
    """

    output = await _get_job_check_owner(
        project_id=project_id,
        job_id=job_id,
        user_id=user.id,
        db=db,
    )
    job = output["job"]
    await db.close()

    if show_tmp_logs and (job.status == JobStatusTypeV2.SUBMITTED):
        try:
            with open(f"{job.working_dir}/{WORKFLOW_LOG_FILENAME}", "r") as f:
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
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> StreamingResponse:
    """
    Download zipped job folder
    """
    output = await _get_job_check_owner(
        project_id=project_id,
        job_id=job_id,
        user_id=user.id,
        db=db,
    )
    job = output["job"]
    zip_name = f"{Path(job.working_dir).name}_archive.zip"
    return StreamingResponse(
        _zip_folder_to_byte_stream_iterator(folder=job.working_dir),
        media_type="application/x-zip-compressed",
        headers={"Content-Disposition": f"attachment;filename={zip_name}"},
    )


@router.get(
    "/project/{project_id}/job/",
    response_model=list[JobReadV2],
)
async def get_job_list(
    project_id: int,
    user: UserOAuth = Depends(current_active_user),
    log: bool = True,
    db: AsyncSession = Depends(get_async_db),
) -> Optional[list[JobReadV2]]:
    """
    Get job list for given project
    """
    project = await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )

    stm = select(JobV2).where(JobV2.project_id == project.id)
    res = await db.execute(stm)
    job_list = res.scalars().all()
    await db.close()
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
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Response:
    """
    Stop execution of a workflow job.
    """

    _check_shutdown_is_supported()

    # Get job from DB
    output = await _get_job_check_owner(
        project_id=project_id,
        job_id=job_id,
        user_id=user.id,
        db=db,
    )
    job = output["job"]

    _write_shutdown_file(job=job)

    return Response(status_code=status.HTTP_202_ACCEPTED)
