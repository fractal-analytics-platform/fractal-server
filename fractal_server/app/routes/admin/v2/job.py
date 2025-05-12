from pathlib import Path

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from fastapi.responses import StreamingResponse
from pydantic.types import AwareDatetime
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import JobV2
from fractal_server.app.models.v2 import ProjectV2
from fractal_server.app.routes.auth import current_active_superuser
from fractal_server.app.routes.aux._job import _write_shutdown_file
from fractal_server.app.routes.aux._runner import _check_shutdown_is_supported
from fractal_server.app.runner.filenames import WORKFLOW_LOG_FILENAME
from fractal_server.app.schemas.v2 import JobReadV2
from fractal_server.app.schemas.v2 import JobStatusTypeV2
from fractal_server.app.schemas.v2 import JobUpdateV2
from fractal_server.utils import get_timestamp
from fractal_server.zip_tools import _zip_folder_to_byte_stream_iterator

router = APIRouter()


@router.get("/", response_model=list[JobReadV2])
async def view_job(
    id: int | None = None,
    user_id: int | None = None,
    project_id: int | None = None,
    dataset_id: int | None = None,
    workflow_id: int | None = None,
    status: JobStatusTypeV2 | None = None,
    start_timestamp_min: AwareDatetime | None = None,
    start_timestamp_max: AwareDatetime | None = None,
    end_timestamp_min: AwareDatetime | None = None,
    end_timestamp_max: AwareDatetime | None = None,
    log: bool = True,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> list[JobReadV2]:
    """
    Query `ApplyWorkflow` table.

    Args:
        id: If not `None`, select a given `applyworkflow.id`.
        project_id: If not `None`, select a given `applyworkflow.project_id`.
        dataset_id: If not `None`, select a given
            `applyworkflow.input_dataset_id`.
        workflow_id: If not `None`, select a given `applyworkflow.workflow_id`.
        status: If not `None`, select a given `applyworkflow.status`.
        start_timestamp_min: If not `None`, select a rows with
            `start_timestamp` after `start_timestamp_min`.
        start_timestamp_max: If not `None`, select a rows with
            `start_timestamp` before `start_timestamp_min`.
        end_timestamp_min: If not `None`, select a rows with `end_timestamp`
            after `end_timestamp_min`.
        end_timestamp_max: If not `None`, select a rows with `end_timestamp`
            before `end_timestamp_min`.
        log: If `True`, include `job.log`, if `False`
            `job.log` is set to `None`.
    """

    stm = select(JobV2)

    if id is not None:
        stm = stm.where(JobV2.id == id)
    if user_id is not None:
        stm = stm.join(ProjectV2).where(
            ProjectV2.user_list.any(UserOAuth.id == user_id)
        )
    if project_id is not None:
        stm = stm.where(JobV2.project_id == project_id)
    if dataset_id is not None:
        stm = stm.where(JobV2.dataset_id == dataset_id)
    if workflow_id is not None:
        stm = stm.where(JobV2.workflow_id == workflow_id)
    if status is not None:
        stm = stm.where(JobV2.status == status)
    if start_timestamp_min is not None:
        start_timestamp_min = start_timestamp_min
        stm = stm.where(JobV2.start_timestamp >= start_timestamp_min)
    if start_timestamp_max is not None:
        start_timestamp_max = start_timestamp_max
        stm = stm.where(JobV2.start_timestamp <= start_timestamp_max)
    if end_timestamp_min is not None:
        end_timestamp_min = end_timestamp_min
        stm = stm.where(JobV2.end_timestamp >= end_timestamp_min)
    if end_timestamp_max is not None:
        end_timestamp_max = end_timestamp_max
        stm = stm.where(JobV2.end_timestamp <= end_timestamp_max)

    res = await db.execute(stm)
    job_list = res.scalars().all()
    await db.close()
    if not log:
        for job in job_list:
            setattr(job, "log", None)

    return job_list


@router.get("/{job_id}/", response_model=JobReadV2)
async def view_single_job(
    job_id: int = None,
    show_tmp_logs: bool = False,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> JobReadV2:

    job = await db.get(JobV2, job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found",
        )
    await db.close()

    if show_tmp_logs and (job.status == JobStatusTypeV2.SUBMITTED):
        try:
            with open(f"{job.working_dir}/{WORKFLOW_LOG_FILENAME}") as f:
                job.log = f.read()
        except FileNotFoundError:
            pass

    return job


@router.patch("/{job_id}/", response_model=JobReadV2)
async def update_job(
    job_update: JobUpdateV2,
    job_id: int,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> JobReadV2 | None:
    """
    Change the status of an existing job.

    This endpoint is only open to superusers, and it does not apply
    project-based access-control to jobs.
    """
    job = await db.get(JobV2, job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found",
        )

    if job_update.status != JobStatusTypeV2.FAILED:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Cannot set job status to {job_update.status}",
        )

    setattr(job, "status", job_update.status)
    setattr(job, "end_timestamp", get_timestamp())
    await db.commit()
    await db.refresh(job)
    await db.close()
    return job


@router.get("/{job_id}/stop/", status_code=202)
async def stop_job(
    job_id: int,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> Response:
    """
    Stop execution of a workflow job.
    """

    _check_shutdown_is_supported()

    job = await db.get(JobV2, job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found",
        )

    _write_shutdown_file(job=job)

    return Response(status_code=status.HTTP_202_ACCEPTED)


@router.get("/{job_id}/download/", response_class=StreamingResponse)
async def download_job_logs(
    job_id: int,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> StreamingResponse:
    """
    Download job folder
    """
    # Get job from DB
    job = await db.get(JobV2, job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found",
        )
    # Create and return byte stream for zipped log folder
    PREFIX_ZIP = Path(job.working_dir).name
    zip_filename = f"{PREFIX_ZIP}_archive.zip"
    return StreamingResponse(
        _zip_folder_to_byte_stream_iterator(folder=job.working_dir),
        media_type="application/x-zip-compressed",
        headers={"Content-Disposition": f"attachment;filename={zip_filename}"},
    )
