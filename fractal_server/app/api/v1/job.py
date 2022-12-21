import json
from io import BytesIO
from pathlib import Path
from typing import Optional
from zipfile import ZIP_DEFLATED
from zipfile import ZipFile

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status
from fastapi.responses import StreamingResponse

from ...db import AsyncSession
from ...db import get_db
from ...models import ApplyWorkflow
from ...models import ApplyWorkflowRead
from ...runner._common import METADATA_FILENAME
from ...security import current_active_user
from ...security import User
from .project import get_project_check_owner


router = APIRouter()


@router.get("/{job_id}", response_model=ApplyWorkflowRead)
async def get_job(
    job_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[ApplyWorkflow]:
    job = await db.get(ApplyWorkflow, job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Job not found"
        )
    await get_project_check_owner(
        project_id=job.project_id, user_id=user.id, db=db
    )

    job_read = ApplyWorkflowRead(**job.dict())

    try:
        metadata_file = Path(job_read.working_dir) / METADATA_FILENAME
        with metadata_file.open("r") as f:
            metadata = json.load(f)
        job_read.history = metadata["history"]
    except (KeyError, FileNotFoundError):
        pass

    return job_read


@router.get("/download/{job_id}", response_class=StreamingResponse)
async def download_job_logs(
    job_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> StreamingResponse:
    job = await db.get(ApplyWorkflow, job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Job not found"
        )
    await get_project_check_owner(
        project_id=job.project_id, user_id=user.id, db=db
    )

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

    return StreamingResponse(
        iter([byte_stream.getvalue()]),
        media_type="application/x-zip-compressed",
        headers={"Content-Disposition": f"attachment;filename={zip_filename}"},
    )
