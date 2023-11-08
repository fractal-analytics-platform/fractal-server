from datetime import datetime as DateTime
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from sqlmodel import select

from ...db import AsyncSession
from ...db import get_db
from ...models import ApplyWorkflow
from ...models import Project
from ...models import Workflow
from ...schemas import ApplyWorkflowRead
from ...schemas import ProjectRead
from ...security import current_active_superuser
from ...security import User

router = APIRouter()


@router.get("/project/")
async def monitor_project(
    id: Optional[int] = None,
    user: User = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_db),
) -> list[ProjectRead]:

    stm = select(Project)

    if id:
        stm = stm.where(Project.id == id)

    res = await db.execute(stm)
    project_list = res.scalars().all()
    await db.close()

    return project_list


@router.get("/workflow/")
async def monitor_workflow(
    id: Optional[int] = None,
    project_id: Optional[int] = None,
    name: Optional[str] = None,
    user: User = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_db),
):
    stm = select(Workflow)

    if id:
        stm = stm.where(Workflow.id == id)
    if project_id:
        stm = stm.where(Workflow.project_id == project_id)
    if name:
        stm = stm.where(Workflow.name.contains(name))

    res = await db.execute(stm)
    project_list = res.scalars().all()
    await db.close()

    return project_list


@router.get("/dataset/")
async def monitor_dataset(
    user: User = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_db),
):
    pass


@router.get("/job/")
async def monitor_job(
    id: Optional[int] = None,
    project_id: Optional[int] = None,
    input_dataset_id: Optional[int] = None,
    output_dataset_id: Optional[int] = None,
    workflow_id: Optional[int] = None,
    working_dir: Optional[str] = None,
    working_dir_user: Optional[str] = None,
    status: Optional[str] = None,
    start_timestamp: Optional[DateTime] = None,
    end_timestamp: Optional[DateTime] = None,
    user: User = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_db),
) -> Optional[list[ApplyWorkflowRead]]:

    stm = select(ApplyWorkflow)

    if id:
        stm = stm.where(ApplyWorkflow.id == id)
    if project_id:
        stm = stm.where(ApplyWorkflow.project_id == project_id)
    if input_dataset_id:
        stm = stm.where(ApplyWorkflow.input_dataset_id == input_dataset_id)
    if output_dataset_id:
        stm = stm.where(ApplyWorkflow.output_dataset_id == output_dataset_id)
    if workflow_id:
        stm = stm.where(ApplyWorkflow.workflow_id == workflow_id)
    if working_dir:
        stm = stm.where(ApplyWorkflow.working_dir == working_dir)
    if working_dir_user:
        stm = stm.where(ApplyWorkflow.working_dir_user == working_dir_user)
    if status:
        stm = stm.where(ApplyWorkflow.status == status)
    if start_timestamp:
        stm = stm.where(ApplyWorkflow.start_timestamp >= start_timestamp)
    if end_timestamp:
        stm = stm.where(ApplyWorkflow.end_timestamp <= end_timestamp)

    res = await db.execute(stm)
    job_list = res.scalars().all()
    await db.close()

    return job_list
