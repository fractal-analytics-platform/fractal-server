import json
from pathlib import Path
from typing import Optional

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

    from devtools import debug

    debug(job_read)

    from io import BytesIO
    from zipfile import ZipFile
    from zipfile import ZIP_DEFLATED
    import os

    io = BytesIO()
    zip_sub_dir = (
        f"{job_read.project_id}_{job_read.workflow_id}_{job_id}_archive"
    )
    zip_filename = "%s.zip" % zip_sub_dir
    with ZipFile(io, mode="w", compression=ZIP_DEFLATED) as zip:
        for fpath in os.listdir(job_read.working_dir):
            zip.write(job_read.working_dir / fpath)
        # close zip
        zip.close()
    return StreamingResponse(
        iter([io.getvalue()]),
        media_type="application/x-zip-compressed",
        headers={"Content-Disposition": f"attachment;filename={zip_filename}"},
    )
