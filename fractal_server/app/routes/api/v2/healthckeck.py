from random import randint

from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import Depends
from fastapi import Request
from fastapi import status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlmodel import select

from ....db import AsyncSession
from ....db import get_async_db
from ....models import TaskGroupV2
from ....models import UserOAuth
from ....schemas.v2 import DatasetCreateV2
from ....schemas.v2 import JobCreateV2
from ....schemas.v2 import ProjectCreateV2
from ....schemas.v2 import TaskCreateV2
from ....schemas.v2 import WorkflowCreateV2
from ....schemas.v2 import WorkflowTaskCreateV2
from ...auth import current_active_user
from .dataset import create_dataset
from .project import create_project
from .submit import apply_workflow
from .task import create_task
from .workflow import create_workflow
from .workflowtask import create_workflowtask

router = APIRouter()


class HealthCheck(BaseModel):
    zarr_dir: str


@router.post("/", status_code=status.HTTP_200_OK)
async def run_healthcheck(
    payload: HealthCheck,
    request: Request,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> JSONResponse:

    random_integer = randint(10**9, 10**10 - 1)  # nosec B311

    # Create a project
    project = await create_project(
        project=ProjectCreateV2(name=f"project_{random_integer}"),
        user=user,
        db=db,
    )

    # Create a dataset
    dataset = await create_dataset(
        project_id=project.id,
        dataset=DatasetCreateV2(
            name=f"dataset_{random_integer}",
            zarr_dir=payload.zarr_dir,
        ),
        user=user,
        db=db,
    )

    # Create a workflow;
    workflow = await create_workflow(
        project_id=project.id,
        workflow=WorkflowCreateV2(name=f"workflow_{random_integer}"),
        user=user,
        db=db,
    )

    # Create (or get) a task
    res = await db.execute(
        select(TaskGroupV2)
        .where(TaskGroupV2.user_id == user.id)
        .where(TaskGroupV2.pkg_name == "__TEST_ECHO_TASK__")
        .where(TaskGroupV2.version == "9.9.9")
    )
    task_group = res.scalar_one_or_none()
    if task_group is None:
        task = await create_task(
            private=True,
            task=TaskCreateV2(
                name="__TEST_ECHO_TASK__",
                version="9.9.9",
                command_non_parallel="echo",
            ),
            user=user,
            db=db,
        )
    else:
        task = task_group.task_list[0]

    await create_workflowtask(
        project_id=project.id,
        workflow_id=workflow.id,
        task_id=task.id,
        wftask=WorkflowTaskCreateV2(),
        user=user,
        db=db,
    )

    job = await apply_workflow(
        project_id=project.id,
        workflow_id=workflow.id,
        dataset_id=dataset.id,
        job_create=JobCreateV2(),
        background_tasks=BackgroundTasks(),
        request=request,
        user=user,
        db=db,
    )
    return JSONResponse(content=job.id, status_code=200)
