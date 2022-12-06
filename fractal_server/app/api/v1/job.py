from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status

from ...db import AsyncSession
from ...db import get_db
from ...models import ApplyWorkflow
from ...models import ApplyWorkflowRead
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
            status_code=status.HTTP_404_NOT_FOUND, detail="Project not found"
        )
    await get_project_check_owner(
        project_id=job.project_id, user_id=user.id, db=db
    )

    return job
