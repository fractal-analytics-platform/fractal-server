from pathlib import Path
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from fastapi import Response
from fastapi import status
from fastapi.responses import StreamingResponse
from sqlmodel import select

from ....db import AsyncSession
from ....db import get_async_db
from ....models.v2 import JobV2
from ....models.v2 import ProjectV2
from ....runner.filenames import WORKFLOW_LOG_FILENAME
from ....schemas.v2 import JobReadV2
from ....schemas.v2 import JobStatusTypeV2
from ....security import current_active_user
from ....security import User
from ...aux._job import _write_shutdown_file
from ...aux._job import _zip_folder_to_byte_stream
from ...aux._runner import _check_shutdown_is_supported
from ._aux_functions import _get_job_check_owner
from ._aux_functions import _get_project_check_owner
from ._aux_functions import _get_workflow_check_owner

router = APIRouter()


@router.get("/job/", response_model=list[JobReadV2])
async def get_user_jobs(
    user: User = Depends(current_active_user),
    log: bool = True,
    db: AsyncSession = Depends(get_async_db),
) -> list[JobReadV2]:
    """
    Returns all the jobs of the current user
    """
    stm = (
        select(JobV2)
        .join(ProjectV2)
        .where(ProjectV2.user_list.any(User.id == user.id))
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
    user: User = Depends(current_active_user),
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
    user: User = Depends(current_active_user),
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
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> StreamingResponse:
    """
    Download job folder
    """
    output = await _get_job_check_owner(
        project_id=project_id,
        job_id=job_id,
        user_id=user.id,
        db=db,
    )
    job = output["job"]

    # Create and return byte stream for zipped log folder
    PREFIX_ZIP = Path(job.working_dir).name
    zip_filename = f"{PREFIX_ZIP}_archive.zip"
    byte_stream = _zip_folder_to_byte_stream(
        folder=job.working_dir, zip_filename=zip_filename
    )
    return StreamingResponse(
        iter([byte_stream.getvalue()]),
        media_type="application/x-zip-compressed",
        headers={"Content-Disposition": f"attachment;filename={zip_filename}"},
    )


@router.get(
    "/project/{project_id}/job/",
    response_model=list[JobReadV2],
)
async def get_job_list(
    project_id: int,
    user: User = Depends(current_active_user),
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
    user: User = Depends(current_active_user),
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
