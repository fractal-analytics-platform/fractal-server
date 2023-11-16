from datetime import datetime as DateTime
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from sqlalchemy import func
from sqlmodel import select

from ...db import AsyncSession
from ...db import get_db
from ...models import ApplyWorkflow
from ...models import Dataset
from ...models import JobStatusType
from ...models import Project
from ...models import Workflow
from ...schemas import ApplyWorkflowRead
from ...schemas import DatasetRead
from ...schemas import ProjectRead
from ...schemas import WorkflowRead
from ...security import current_active_superuser
from ...security import User


router = APIRouter()


@router.get("/project/", response_model=list[ProjectRead])
async def monitor_project(
    id: Optional[int] = None,
    user_id: Optional[int] = None,
    user: User = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_db),
) -> list[ProjectRead]:

    stm = select(Project)

    if id is not None:
        stm = stm.where(Project.id == id)

    if user_id is not None:
        stm = stm.where(Project.user_list.any(User.id == user_id))

    res = await db.execute(stm)
    project_list = res.scalars().all()
    await db.close()

    return project_list


@router.get("/workflow/", response_model=list[WorkflowRead])
async def monitor_workflow(
    id: Optional[int] = None,
    project_id: Optional[int] = None,
    name_contains: Optional[str] = None,
    user: User = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_db),
) -> list[WorkflowRead]:
    stm = select(Workflow)

    if id is not None:
        stm = stm.where(Workflow.id == id)
    if project_id is not None:
        stm = stm.where(Workflow.project_id == project_id)
    if name_contains is not None:
        # SQLAlchemy2: use icontains
        stm = stm.where(
            func.lower(Workflow.name).contains(name_contains.lower())
        )

    res = await db.execute(stm)
    workflow_list = res.scalars().all()
    await db.close()

    return workflow_list


@router.get("/dataset/", response_model=list[DatasetRead])
async def monitor_dataset(
    id: Optional[int] = None,
    project_id: Optional[int] = None,
    name_contains: Optional[str] = None,
    type: Optional[str] = None,
    user: User = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_db),
) -> list[DatasetRead]:
    stm = select(Dataset)

    if id is not None:
        stm = stm.where(Dataset.id == id)
    if project_id is not None:
        stm = stm.where(Dataset.project_id == project_id)
    if name_contains is not None:
        # SQLAlchemy2: use icontains
        stm = stm.where(
            func.lower(Dataset.name).contains(name_contains.lower())
        )
    if type is not None:
        stm = stm.where(Dataset.type == type)

    res = await db.execute(stm)
    dataset_list = res.scalars().all()
    await db.close()

    return dataset_list


@router.get("/job/", response_model=list[ApplyWorkflowRead])
async def monitor_job(
    id: Optional[int] = None,
    project_id: Optional[int] = None,
    input_dataset_id: Optional[int] = None,
    output_dataset_id: Optional[int] = None,
    workflow_id: Optional[int] = None,
    status: Optional[JobStatusType] = None,
    start_timestamp_min: Optional[DateTime] = None,
    start_timestamp_max: Optional[DateTime] = None,
    end_timestamp_min: Optional[DateTime] = None,
    end_timestamp_max: Optional[DateTime] = None,
    user: User = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_db),
) -> list[ApplyWorkflowRead]:

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
    if status is not None:
        stm = stm.where(ApplyWorkflow.status == status)
    if start_timestamp_min is not None:
        stm = stm.where(ApplyWorkflow.start_timestamp >= start_timestamp_min)
    if start_timestamp_max is not None:
        stm = stm.where(ApplyWorkflow.start_timestamp <= start_timestamp_max)
    if end_timestamp_min is not None:
        stm = stm.where(ApplyWorkflow.end_timestamp >= end_timestamp_min)
    if end_timestamp_max is not None:
        stm = stm.where(ApplyWorkflow.end_timestamp <= end_timestamp_max)

    res = await db.execute(stm)
    job_list = res.scalars().all()
    await db.close()

    return job_list
