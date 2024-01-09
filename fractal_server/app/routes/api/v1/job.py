from pathlib import Path
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from fastapi import Response
from fastapi import status
from fastapi.responses import StreamingResponse
from sqlmodel import select

from ....db import AsyncSession
from ....db import get_db
from ....models import ApplyWorkflow
from ....models import Project
from ....schemas import ApplyWorkflowRead
from ....security import current_active_user
from ....security import User
from ...aux._job import _write_shutdown_file
from ...aux._job import _zip_folder_to_byte_stream
from ...aux._runner import _check_backend_is_slurm
from ._aux_functions import _get_job_check_owner
from ._aux_functions import _get_project_check_owner
from ._aux_functions import _get_workflow_check_owner


router = APIRouter()


@router.get("/job/", response_model=list[ApplyWorkflowRead])
async def get_user_jobs(
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> list[ApplyWorkflowRead]:
    """
    Returns all the jobs of the current user
    """

    stm = select(ApplyWorkflow)
    stm = stm.join(Project).where(Project.user_list.any(User.id == user.id))
    res = await db.execute(stm)
    job_list = res.scalars().all()

    return job_list


@router.get(
    "/project/{project_id}/workflow/{workflow_id}/job/",
    response_model=list[ApplyWorkflowRead],
)
async def get_workflow_jobs(
    project_id: int,
    workflow_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[list[ApplyWorkflowRead]]:
    """
    Returns all the jobs related to a specific workflow
    """
    await _get_workflow_check_owner(
        project_id=project_id, workflow_id=workflow_id, user_id=user.id, db=db
    )
    stm = select(ApplyWorkflow).where(ApplyWorkflow.workflow_id == workflow_id)
    res = await db.execute(stm)
    job_list = res.scalars().all()
    return job_list


@router.get(
    "/project/{project_id}/job/{job_id}/",
    response_model=ApplyWorkflowRead,
)
async def read_job(
    project_id: int,
    job_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[ApplyWorkflowRead]:
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
    return job


@router.get(
    "/project/{project_id}/job/{job_id}/download/",
    response_class=StreamingResponse,
)
async def download_job_logs(
    project_id: int,
    job_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
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
    response_model=list[ApplyWorkflowRead],
)
async def get_job_list(
    project_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[list[ApplyWorkflowRead]]:
    """
    Get job list for given project
    """
    project = await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )

    stm = select(ApplyWorkflow).where(ApplyWorkflow.project_id == project.id)
    res = await db.execute(stm)
    job_list = res.scalars().all()

    return job_list


@router.get(
    "/project/{project_id}/job/{job_id}/stop/",
    status_code=204,
)
async def stop_job(
    project_id: int,
    job_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Response:
    """
    Stop execution of a workflow job (only available for slurm backend)
    """

    # This endpoint is only implemented for SLURM backend
    _check_backend_is_slurm()

    # Get job from DB
    output = await _get_job_check_owner(
        project_id=project_id,
        job_id=job_id,
        user_id=user.id,
        db=db,
    )
    job = output["job"]

    _write_shutdown_file(job=job)

    return Response(status_code=status.HTTP_204_NO_CONTENT)
