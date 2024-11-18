"""
Definition of `/admin` routes.
"""
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from fastapi.responses import StreamingResponse
from sqlalchemy import func
from sqlmodel import select

from ....utils import get_timestamp
from ....zip_tools import _zip_folder_to_byte_stream_iterator
from ...db import AsyncSession
from ...db import get_async_db
from ...models.v1 import ApplyWorkflow
from ...models.v1 import Dataset
from ...models.v1 import JobStatusTypeV1
from ...models.v1 import Project
from ...models.v1 import Workflow
from ...runner.filenames import WORKFLOW_LOG_FILENAME
from ...schemas.v1 import ApplyWorkflowReadV1
from ...schemas.v1 import ApplyWorkflowUpdateV1
from ...schemas.v1 import DatasetReadV1
from ...schemas.v1 import ProjectReadV1
from ...schemas.v1 import WorkflowReadV1
from ..aux._job import _write_shutdown_file
from ..aux._runner import _check_shutdown_is_supported
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth import current_active_superuser
from fractal_server.app.routes.aux import _raise_if_naive_datetime

router_admin_v1 = APIRouter()


@router_admin_v1.get("/project/", response_model=list[ProjectReadV1])
async def view_project(
    id: Optional[int] = None,
    user_id: Optional[int] = None,
    timestamp_created_min: Optional[datetime] = None,
    timestamp_created_max: Optional[datetime] = None,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> list[ProjectReadV1]:
    """
    Query `project` table.

    Args:
        id: If not `None`, select a given `project.id`.
        user_id: If not `None`, select a given `project.user_id`.
    """
    _raise_if_naive_datetime(timestamp_created_min, timestamp_created_max)

    stm = select(Project)

    if id is not None:
        stm = stm.where(Project.id == id)

    if user_id is not None:
        stm = stm.where(Project.user_list.any(UserOAuth.id == user_id))
    if timestamp_created_min is not None:
        stm = stm.where(Project.timestamp_created >= timestamp_created_min)
    if timestamp_created_max is not None:
        stm = stm.where(Project.timestamp_created <= timestamp_created_max)

    res = await db.execute(stm)
    project_list = res.scalars().all()
    await db.close()

    return project_list


@router_admin_v1.get("/workflow/", response_model=list[WorkflowReadV1])
async def view_workflow(
    id: Optional[int] = None,
    user_id: Optional[int] = None,
    project_id: Optional[int] = None,
    name_contains: Optional[str] = None,
    timestamp_created_min: Optional[datetime] = None,
    timestamp_created_max: Optional[datetime] = None,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> list[WorkflowReadV1]:
    """
    Query `workflow` table.

    Args:
        id: If not `None`, select a given `workflow.id`.
        project_id: If not `None`, select a given `workflow.project_id`.
        name_contains: If not `None`, select workflows such that their
            `name` attribute contains `name_contains` (case-insensitive).
    """
    _raise_if_naive_datetime(timestamp_created_min, timestamp_created_max)

    stm = select(Workflow)

    if user_id is not None:
        stm = stm.join(Project).where(
            Project.user_list.any(UserOAuth.id == user_id)
        )
    if id is not None:
        stm = stm.where(Workflow.id == id)
    if project_id is not None:
        stm = stm.where(Workflow.project_id == project_id)
    if name_contains is not None:
        # SQLAlchemy2: use icontains
        stm = stm.where(
            func.lower(Workflow.name).contains(name_contains.lower())
        )
    if timestamp_created_min is not None:
        stm = stm.where(Workflow.timestamp_created >= timestamp_created_min)
    if timestamp_created_max is not None:
        stm = stm.where(Workflow.timestamp_created <= timestamp_created_max)

    res = await db.execute(stm)
    workflow_list = res.scalars().all()
    await db.close()

    return workflow_list


@router_admin_v1.get("/dataset/", response_model=list[DatasetReadV1])
async def view_dataset(
    id: Optional[int] = None,
    user_id: Optional[int] = None,
    project_id: Optional[int] = None,
    name_contains: Optional[str] = None,
    type: Optional[str] = None,
    timestamp_created_min: Optional[datetime] = None,
    timestamp_created_max: Optional[datetime] = None,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> list[DatasetReadV1]:
    """
    Query `dataset` table.

    Args:
        id: If not `None`, select a given `dataset.id`.
        project_id: If not `None`, select a given `dataset.project_id`.
        name_contains: If not `None`, select datasets such that their
            `name` attribute contains `name_contains` (case-insensitive).
        type: If not `None`, select a given `dataset.type`.
    """
    _raise_if_naive_datetime(timestamp_created_min, timestamp_created_max)

    stm = select(Dataset)

    if user_id is not None:
        stm = stm.join(Project).where(
            Project.user_list.any(UserOAuth.id == user_id)
        )
    if id is not None:
        stm = stm.where(Dataset.id == id)
    if project_id is not None:
        stm = stm.where(Dataset.project_id == project_id)
    if name_contains is not None:
        # SQLAlchemy2: use icontains
        stm = stm.where(
            func.lower(Dataset.name).contains(name_contains.lower())
        )
    if type is not None:
        stm = stm.where(Dataset.type == type)
    if timestamp_created_min is not None:
        stm = stm.where(Dataset.timestamp_created >= timestamp_created_min)
    if timestamp_created_max is not None:
        stm = stm.where(Dataset.timestamp_created <= timestamp_created_max)

    res = await db.execute(stm)
    dataset_list = res.scalars().all()
    await db.close()

    return dataset_list


@router_admin_v1.get("/job/", response_model=list[ApplyWorkflowReadV1])
async def view_job(
    id: Optional[int] = None,
    user_id: Optional[int] = None,
    project_id: Optional[int] = None,
    input_dataset_id: Optional[int] = None,
    output_dataset_id: Optional[int] = None,
    workflow_id: Optional[int] = None,
    status: Optional[JobStatusTypeV1] = None,
    start_timestamp_min: Optional[datetime] = None,
    start_timestamp_max: Optional[datetime] = None,
    end_timestamp_min: Optional[datetime] = None,
    end_timestamp_max: Optional[datetime] = None,
    log: bool = True,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> list[ApplyWorkflowReadV1]:
    """
    Query `ApplyWorkflow` table.

    Args:
        id: If not `None`, select a given `applyworkflow.id`.
        project_id: If not `None`, select a given `applyworkflow.project_id`.
        input_dataset_id: If not `None`, select a given
            `applyworkflow.input_dataset_id`.
        output_dataset_id: If not `None`, select a given
            `applyworkflow.output_dataset_id`.
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
    _raise_if_naive_datetime(
        start_timestamp_min,
        start_timestamp_max,
        end_timestamp_min,
        end_timestamp_max,
    )

    stm = select(ApplyWorkflow)

    if id is not None:
        stm = stm.where(ApplyWorkflow.id == id)
    if user_id is not None:
        stm = stm.join(Project).where(
            Project.user_list.any(UserOAuth.id == user_id)
        )
    if project_id is not None:
        stm = stm.where(ApplyWorkflow.project_id == project_id)
    if input_dataset_id is not None:
        stm = stm.where(ApplyWorkflow.input_dataset_id == input_dataset_id)
    if output_dataset_id is not None:
        stm = stm.where(ApplyWorkflow.output_dataset_id == output_dataset_id)
    if workflow_id is not None:
        stm = stm.where(ApplyWorkflow.workflow_id == workflow_id)
    if status is not None:
        stm = stm.where(ApplyWorkflow.status == status)
    if start_timestamp_min is not None:
        stm = stm.where(ApplyWorkflow.start_timestamp >= start_timestamp_min)
    if start_timestamp_max is not None:
        stm = stm.where(ApplyWorkflow.start_timestamp <= start_timestamp_max)
    if end_timestamp_min is not None:
        stm = stm.where(ApplyWorkflow.end_timestamp >= end_timestamp_min)
    if end_timestamp_max is not None:
        stm = stm.where(ApplyWorkflow.end_timestamp <= end_timestamp_max)

    res = await db.execute(stm)
    job_list = res.scalars().all()
    await db.close()
    if not log:
        for job in job_list:
            setattr(job, "log", None)

    return job_list


@router_admin_v1.get("/job/{job_id}/", response_model=ApplyWorkflowReadV1)
async def view_single_job(
    job_id: int = None,
    show_tmp_logs: bool = False,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> ApplyWorkflowReadV1:

    job = await db.get(ApplyWorkflow, job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found",
        )
    await db.close()

    if show_tmp_logs and (job.status == JobStatusTypeV1.SUBMITTED):
        try:
            with open(f"{job.working_dir}/{WORKFLOW_LOG_FILENAME}", "r") as f:
                job.log = f.read()
        except FileNotFoundError:
            pass

    return job


@router_admin_v1.patch(
    "/job/{job_id}/",
    response_model=ApplyWorkflowReadV1,
)
async def update_job(
    job_update: ApplyWorkflowUpdateV1,
    job_id: int,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[ApplyWorkflowReadV1]:
    """
    Change the status of an existing job.

    This endpoint is only open to superusers, and it does not apply
    project-based access-control to jobs.
    """
    job = await db.get(ApplyWorkflow, job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found",
        )

    if job_update.status != JobStatusTypeV1.FAILED:
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


@router_admin_v1.get("/job/{job_id}/stop/", status_code=202)
async def stop_job(
    job_id: int,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> Response:
    """
    Stop execution of a workflow job.
    """

    _check_shutdown_is_supported()

    job = await db.get(ApplyWorkflow, job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found",
        )

    _write_shutdown_file(job=job)

    return Response(status_code=status.HTTP_202_ACCEPTED)


@router_admin_v1.get(
    "/job/{job_id}/download/",
    response_class=StreamingResponse,
)
async def download_job_logs(
    job_id: int,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> StreamingResponse:
    """
    Download job folder
    """
    # Get job from DB
    job = await db.get(ApplyWorkflow, job_id)
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
