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

from ...utils import get_timestamp
from ..db import AsyncSession
from ..db import get_db
from ..models import ApplyWorkflow
from ..models import Dataset
from ..models import JobStatusType
from ..models import Project
from ..models import Workflow
from ..models.security import UserOAuth as User
from ..schemas import ApplyWorkflowRead
from ..schemas import ApplyWorkflowUpdate
from ..schemas import DatasetRead
from ..schemas import ProjectRead
from ..schemas import WorkflowRead
from ..security import current_active_superuser
from .aux._job import _write_shutdown_file
from .aux._job import _zip_folder_to_byte_stream
from .aux._runner import _check_backend_is_slurm

router_admin = APIRouter()


@router_admin.get("/project/", response_model=list[ProjectRead])
async def view_project(
    id: Optional[int] = None,
    user_id: Optional[int] = None,
    user: User = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_db),
) -> list[ProjectRead]:
    """
    Query `project` table.

    Args:
        id: If not `None`, select a given `project.id`.
        user_id: If not `None`, select a given `project.user_id`.
    """

    stm = select(Project)

    if id is not None:
        stm = stm.where(Project.id == id)

    if user_id is not None:
        stm = stm.where(Project.user_list.any(User.id == user_id))

    res = await db.execute(stm)
    project_list = res.scalars().all()
    await db.close()

    return project_list


@router_admin.get("/workflow/", response_model=list[WorkflowRead])
async def view_workflow(
    id: Optional[int] = None,
    user_id: Optional[int] = None,
    project_id: Optional[int] = None,
    name_contains: Optional[str] = None,
    user: User = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_db),
) -> list[WorkflowRead]:
    """
    Query `workflow` table.

    Args:
        id: If not `None`, select a given `workflow.id`.
        project_id: If not `None`, select a given `workflow.project_id`.
        name_contains: If not `None`, select workflows such that their
            `name` attribute contains `name_contains` (case-insensitive).
    """
    stm = select(Workflow)

    if user_id is not None:
        stm = stm.join(Project).where(
            Project.user_list.any(User.id == user_id)
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

    res = await db.execute(stm)
    workflow_list = res.scalars().all()
    await db.close()

    return workflow_list


@router_admin.get("/dataset/", response_model=list[DatasetRead])
async def view_dataset(
    id: Optional[int] = None,
    user_id: Optional[int] = None,
    project_id: Optional[int] = None,
    name_contains: Optional[str] = None,
    type: Optional[str] = None,
    user: User = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_db),
) -> list[DatasetRead]:
    """
    Query `dataset` table.

    Args:
        id: If not `None`, select a given `dataset.id`.
        project_id: If not `None`, select a given `dataset.project_id`.
        name_contains: If not `None`, select datasets such that their
            `name` attribute contains `name_contains` (case-insensitive).
        type: If not `None`, select a given `dataset.type`.
    """
    stm = select(Dataset)

    if user_id is not None:
        stm = stm.join(Project).where(
            Project.user_list.any(User.id == user_id)
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

    res = await db.execute(stm)
    dataset_list = res.scalars().all()
    await db.close()

    return dataset_list


@router_admin.get("/job/", response_model=list[ApplyWorkflowRead])
async def view_job(
    id: Optional[int] = None,
    user_id: Optional[int] = None,
    project_id: Optional[int] = None,
    input_dataset_id: Optional[int] = None,
    output_dataset_id: Optional[int] = None,
    workflow_id: Optional[int] = None,
    status: Optional[JobStatusType] = None,
    start_timestamp_min: Optional[datetime] = None,
    start_timestamp_max: Optional[datetime] = None,
    end_timestamp_min: Optional[datetime] = None,
    end_timestamp_max: Optional[datetime] = None,
    user: User = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_db),
) -> list[ApplyWorkflowRead]:
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
    """
    stm = select(ApplyWorkflow)

    if id is not None:
        stm = stm.where(ApplyWorkflow.id == id)
    if user_id is not None:
        stm = stm.join(Project).where(
            Project.user_list.any(User.id == user_id)
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

    return job_list


@router_admin.patch(
    "/job/{job_id}/",
    response_model=ApplyWorkflowRead,
)
async def update_job(
    job_update: ApplyWorkflowUpdate,
    job_id: int,
    user: User = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_db),
) -> Optional[ApplyWorkflowRead]:
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

    if job_update.status != JobStatusType.FAILED:
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


@router_admin.get("/job/{job_id}/stop/", status_code=204)
async def stop_job(
    job_id: int,
    user: User = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_db),
) -> Response:
    """
    Stop execution of a workflow job.

    Only available for slurm backend.
    """

    _check_backend_is_slurm()

    job = await db.get(ApplyWorkflow, job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found",
        )

    _write_shutdown_file(job=job)

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router_admin.get(
    "/job/{job_id}/download/",
    response_class=StreamingResponse,
)
async def download_job_logs(
    job_id: int,
    user: User = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_db),
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
    byte_stream = _zip_folder_to_byte_stream(
        folder=job.working_dir, zip_filename=zip_filename
    )
    return StreamingResponse(
        iter([byte_stream.getvalue()]),
        media_type="application/x-zip-compressed",
        headers={"Content-Disposition": f"attachment;filename={zip_filename}"},
    )
