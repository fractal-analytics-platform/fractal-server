from fastapi import APIRouter
from fastapi import Depends

from ...db import AsyncSession
from ...db import get_db
from ...models import ApplyWorkflow
from ...models import ApplyWorkflowRead
from ...security import current_active_user
from ...security import User


router = APIRouter()


@router.get("/{job_id}", response_model=ApplyWorkflowRead)
async def get_job(
    job_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> ApplyWorkflow:
    # FIXME acl

    job = await db.get(ApplyWorkflow, job_id)
    return job
