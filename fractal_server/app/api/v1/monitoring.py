from fastapi import APIRouter
from fastapi import Depends

from ...db import AsyncSession
from ...db import get_db
from ...security import current_active_superuser
from ...security import User

router = APIRouter()


@router.get("/project/")
async def monitor_project(
    user: User = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_db),
):
    pass


@router.get("/workflow/")
async def monitor_workflow(
    user: User = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_db),
):
    pass


@router.get("/dataset/")
async def monitor_dataset(
    user: User = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_db),
):
    pass


@router.get("/job/")
async def monitor_job(
    user: User = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_db),
):
    pass
