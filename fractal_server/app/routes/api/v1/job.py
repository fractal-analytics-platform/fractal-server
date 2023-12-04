from io import BytesIO
from pathlib import Path
from typing import Optional
from zipfile import ZIP_DEFLATED
from zipfile import ZipFile

from fastapi import APIRouter
from fastapi import Depends
from fastapi import Response
from fastapi import status
from fastapi.responses import StreamingResponse

from ....db import AsyncSession
from ....db import get_db
from ....schemas import ApplyWorkflowRead
from ....security import current_active_user
from ....security import User
from ._aux_functions import _get_job_check_owner
from ._aux_functions import _get_project_check_owner
from ._aux_functions import _get_workflow_check_owner
from ._aux_functions import _only_slurm
from ._aux_functions import _stop_job


router = APIRouter()


@router.get("/job/", response_model=list[ApplyWorkflowRead])
async def get_user_jobs(
    user: User = Depends(current_active_user),
) -> list[ApplyWorkflowRead]:
    """
    Returns all the jobs of the current user
    """

    job_list = [
        job for project in user.project_list for job in project.job_list
    ]

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

    workflow = await _get_workflow_check_owner(
        project_id=project_id, workflow_id=workflow_id, user_id=user.id, db=db
    )
    job_list = workflow.job_list
    await db.close()

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

    # Extract job's working_dir attribute
    working_dir_str = job.dict()["working_dir"]
    working_dir_path = Path(working_dir_str)

    # Create zip byte stream
    PREFIX_ZIP = working_dir_path.name
    zip_filename = f"{PREFIX_ZIP}_archive.zip"
    byte_stream = BytesIO()
    with ZipFile(byte_stream, mode="w", compression=ZIP_DEFLATED) as zipfile:
        for fpath in working_dir_path.glob("*"):
            zipfile.write(filename=str(fpath), arcname=str(fpath.name))

    await db.close()

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
    return project.job_list


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
    _only_slurm()

    # Get job from DB
    output = await _get_job_check_owner(
        project_id=project_id,
        job_id=job_id,
        user_id=user.id,
        db=db,
    )
    job = output["job"]

    _stop_job(job)

    return Response(status_code=status.HTTP_204_NO_CONTENT)
