from datetime import datetime as DateTime
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from sqlmodel import select

from ...db import AsyncSession
from ...db import get_db
from ...models import ApplyWorkflow
from ...models import Dataset
from ...models import Project
from ...models import Workflow
from ...schemas import ApplyWorkflowRead
from ...schemas import ProjectRead
from ...security import current_active_superuser
from ...security import User
from fractal_server.app.schemas.workflow import WorkflowTaskStatusType

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

    if id is not None:
        stm = stm.where(Workflow.id == id)
    if project_id is not None:
        stm = stm.where(Workflow.project_id == project_id)
    if name is not None:
        stm = stm.where(Workflow.name.contains(name))

    res = await db.execute(stm)
    project_list = res.scalars().all()
    await db.close()

    return project_list


@router.get("/dataset/")
async def monitor_dataset(
    id: Optional[int] = None,
    project_id: Optional[int] = None,
    name: Optional[str] = None,
    type: Optional[str] = None,
    read_only: Optional[bool] = None,
    user: User = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_db),
):
    stm = select(Dataset)

    if id is not None:
        stm = stm.where(Dataset.id == id)
    if project_id is not None:
        stm = stm.where(Dataset.project_id == project_id)
    if name is not None:
        stm = stm.where(Dataset.name.contains(name))
    if type is not None:
        stm = stm.where(Dataset.type == type)
    if read_only is not None:
        stm = stm.where(Dataset.read_only == read_only)

    res = await db.execute(stm)
    dataset_list = res.scalars().all()
    await db.close()

    return dataset_list


@router.get("/job/")
async def monitor_job(
    id: Optional[int] = None,
    project_id: Optional[int] = None,
    input_dataset_id: Optional[int] = None,
    output_dataset_id: Optional[int] = None,
    workflow_id: Optional[int] = None,
    working_dir: Optional[str] = None,
    working_dir_user: Optional[str] = None,
    status: Optional[WorkflowTaskStatusType] = None,
    start_timestamp: Optional[DateTime] = None,
    end_timestamp: Optional[DateTime] = None,
    user: User = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_db),
) -> Optional[list[ApplyWorkflowRead]]:

    stm = select(ApplyWorkflow)

    if id is not None:
        stm = stm.where(ApplyWorkflow.id == id)
    if project_id is not None:
        stm = stm.where(ApplyWorkflow.project_id == project_id)
    if input_dataset_id is not None:
        stm = stm.where(ApplyWorkflow.input_dataset_id == input_dataset_id)
    if output_dataset_id is not None:
        stm = stm.where(ApplyWorkflow.output_dataset_id == output_dataset_id)
    if workflow_id is not None:
        stm = stm.where(ApplyWorkflow.workflow_id == workflow_id)
    if working_dir is not None:
        stm = stm.where(ApplyWorkflow.working_dir.contains(working_dir))
    if working_dir_user is not None:
        stm = stm.where(
            ApplyWorkflow.working_dir_user.contains(working_dir_user)
        )
    if status is not None:
        stm = stm.where(ApplyWorkflow.status == status)
    if start_timestamp is not None:
        stm = stm.where(ApplyWorkflow.start_timestamp >= start_timestamp)
    if end_timestamp is not None:
        stm = stm.where(ApplyWorkflow.end_timestamp <= end_timestamp)

    res = await db.execute(stm)
    job_list = res.scalars().all()
    await db.close()

    return job_list
