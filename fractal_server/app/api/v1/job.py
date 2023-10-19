from io import BytesIO
from pathlib import Path
from typing import Optional
from zipfile import ZIP_DEFLATED
from zipfile import ZipFile

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from fastapi.responses import StreamingResponse
from sqlmodel import select

from ....config import get_settings
from ....syringe import Inject
from ...db import AsyncSession
from ...db import get_db
from ...models import ApplyWorkflow
from ...models import ArchivedApplyWorkflow
from ...runner._common import SHUTDOWN_FILENAME
from ...schemas import ApplyWorkflowRead
from ...security import current_active_user
from ...security import User
from ._aux_functions import _get_archived_job_check_owner
from ._aux_functions import _get_job_check_owner
from ._aux_functions import _get_project_check_owner

router = APIRouter()


@router.get(
    "/project/{project_id}/job/{job_id}",
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
    Get list of jobs associated to the current project
    """
    await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )
    stm = select(ApplyWorkflow).where(ApplyWorkflow.project_id == project_id)
    res = await db.execute(stm)
    job_list = res.scalars().all()
    await db.close()
    return job_list


@router.get(
    "/project/{project_id}/job/{job_id}/stop/",
    status_code=200,
)
async def stop_job(
    project_id: int,
    job_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[ApplyWorkflow]:
    """
    Stop execution of a workflow job (only available for slurm backend)
    """

    # This endpoint is only implemented for SLURM backend
    settings = Inject(get_settings)
    backend = settings.FRACTAL_RUNNER_BACKEND
    if backend != "slurm":
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "Stopping a job execution is not implemented for "
                f"FRACTAL_RUNNER_BACKEND={backend}."
            ),
        )

    # Get job from DB
    output = await _get_job_check_owner(
        project_id=project_id,
        job_id=job_id,
        user_id=user.id,
        db=db,
    )
    job = output["job"]

    # Note: we are **not** marking the job as failed (by setting its `status`
    # attribute) here, since this will be done by the runner backend as soon as
    # it detects the shutdown-trigerring file and performs the actual shutdown.

    # Write shutdown file
    shutdown_file = Path(job.working_dir) / SHUTDOWN_FILENAME
    with shutdown_file.open("w") as f:
        f.write(f"Trigger executor shutdown for {job.id=}, {project_id=}.")

    return job


@router.post(
    "/project/{project_id}/job/{job_id}/archive/",
    status_code=201,
    response_model=ArchivedApplyWorkflow,
)
async def archive_job(
    project_id: int,
    job_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[ArchivedApplyWorkflow]:

    output = await _get_job_check_owner(
        project_id=project_id,
        job_id=job_id,
        user_id=user.id,
        db=db,
    )
    job = output["job"]

    if job.archived:
        return Response(status_code=status.HTTP_409_CONFLICT)

    # For old data with dump==None
    if not job.input_dataset_dump:
        job.input_dataset_dump = job.input_dataset.dict()
    if not job.output_dataset_dump:
        job.output_dataset_dump = job.output_dataset.dict()

    job.archived = True

    archived_job = ArchivedApplyWorkflow(
        project_id=project_id,
        workflow_dump=job.workflow_dump,
        input_dataset_dump=job.input_dataset_dump,
        output_dataset_dump=job.output_dataset_dump,
        start_timestamp=job.start_timestamp,
        end_timestamp=job.end_timestamp,
    )

    db.add(job)
    db.add(archived_job)
    await db.commit()
    await db.refresh(archived_job)
    await db.close()

    return archived_job


@router.get(
    "/project/{project_id}/archived_job/",
    status_code=200,
    response_model=list[ArchivedApplyWorkflow],
)
async def get_archived_job_list(
    project_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[ArchivedApplyWorkflow]:
    await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )
    res = await db.execute(select(ArchivedApplyWorkflow))
    archive = res.scalars().all()
    await db.close()
    return archive


@router.get(
    "/project/{project_id}/archived_job/{archived_job_id}",
    status_code=200,
    response_model=ArchivedApplyWorkflow,
)
async def get_archived_job(
    project_id: int,
    archived_job_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[ArchivedApplyWorkflow]:
    archived_job = await _get_archived_job_check_owner(
        project_id=project_id,
        archived_job_id=archived_job_id,
        user_id=user.id,
        db=db,
    )
    return archived_job
