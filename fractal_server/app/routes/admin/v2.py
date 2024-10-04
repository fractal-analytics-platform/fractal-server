"""
Definition of `/admin` routes.
"""
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Literal
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from pydantic import EmailStr
from pydantic import Field
from sqlalchemy.sql.operators import is_
from sqlalchemy.sql.operators import is_not
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import JobV2
from fractal_server.app.models.v2 import ProjectV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.app.models.v2 import WorkflowV2
from fractal_server.app.routes.auth import current_active_superuser
from fractal_server.app.routes.auth._aux_auth import (
    _verify_user_belongs_to_group,
)
from fractal_server.app.routes.aux._job import _write_shutdown_file
from fractal_server.app.routes.aux._runner import _check_shutdown_is_supported
from fractal_server.app.runner.filenames import WORKFLOW_LOG_FILENAME
from fractal_server.app.schemas.v2 import JobReadV2
from fractal_server.app.schemas.v2 import JobStatusTypeV2
from fractal_server.app.schemas.v2 import JobUpdateV2
from fractal_server.app.schemas.v2 import ProjectReadV2
from fractal_server.app.schemas.v2 import TaskGroupReadV2
from fractal_server.app.schemas.v2 import TaskGroupUpdateV2
from fractal_server.config import get_settings
from fractal_server.syringe import Inject
from fractal_server.utils import get_timestamp
from fractal_server.zip_tools import _zip_folder_to_byte_stream_iterator

router_admin_v2 = APIRouter()


def _convert_to_db_timestamp(dt: datetime) -> datetime:
    """
    This function takes a timezone-aware datetime and converts it to UTC.
    If using SQLite, it also removes the timezone information in order to make
    the datetime comparable with datetimes in the database.
    """
    if dt.tzinfo is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"The timestamp provided has no timezone information: {dt}",
        )
    _dt = dt.astimezone(timezone.utc)
    if Inject(get_settings).DB_ENGINE == "sqlite":
        return _dt.replace(tzinfo=None)
    return _dt


@router_admin_v2.get("/project/", response_model=list[ProjectReadV2])
async def view_project(
    id: Optional[int] = None,
    user_id: Optional[int] = None,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> list[ProjectReadV2]:
    """
    Query `ProjectV2` table.

    Args:
        id: If not `None`, select a given `project.id`.
        user_id: If not `None`, select a given `project.user_id`.
    """

    stm = select(ProjectV2)

    if id is not None:
        stm = stm.where(ProjectV2.id == id)
    if user_id is not None:
        stm = stm.where(ProjectV2.user_list.any(UserOAuth.id == user_id))

    res = await db.execute(stm)
    project_list = res.scalars().all()
    await db.close()

    return project_list


@router_admin_v2.get("/job/", response_model=list[JobReadV2])
async def view_job(
    id: Optional[int] = None,
    user_id: Optional[int] = None,
    project_id: Optional[int] = None,
    dataset_id: Optional[int] = None,
    workflow_id: Optional[int] = None,
    status: Optional[JobStatusTypeV2] = None,
    start_timestamp_min: Optional[datetime] = None,
    start_timestamp_max: Optional[datetime] = None,
    end_timestamp_min: Optional[datetime] = None,
    end_timestamp_max: Optional[datetime] = None,
    log: bool = True,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> list[JobReadV2]:
    """
    Query `ApplyWorkflow` table.

    Args:
        id: If not `None`, select a given `applyworkflow.id`.
        project_id: If not `None`, select a given `applyworkflow.project_id`.
        dataset_id: If not `None`, select a given
            `applyworkflow.input_dataset_id`.
        workflow_id: If not `None`, select a given `applyworkflow.workflow_id`.
        status: If not `None`, select a given `applyworkflow.status`.
        start_timestamp_min: If not `None`, select a rows with
            `start_timestamp` after `start_timestamp_min`.
        start_timestamp_max: If not `None`, select a rows with
            `start_timestamp` before `start_timestamp_min`.
        end_timestamp_min: If not `None`, select a rows with `end_timestamp`
            after `end_timestamp_min`.
        end_timestamp_max: If not `None`, select a rows with `end_timestamp`
            before `end_timestamp_min`.
        log: If `True`, include `job.log`, if `False`
            `job.log` is set to `None`.
    """
    stm = select(JobV2)

    if id is not None:
        stm = stm.where(JobV2.id == id)
    if user_id is not None:
        stm = stm.join(ProjectV2).where(
            ProjectV2.user_list.any(UserOAuth.id == user_id)
        )
    if project_id is not None:
        stm = stm.where(JobV2.project_id == project_id)
    if dataset_id is not None:
        stm = stm.where(JobV2.dataset_id == dataset_id)
    if workflow_id is not None:
        stm = stm.where(JobV2.workflow_id == workflow_id)
    if status is not None:
        stm = stm.where(JobV2.status == status)
    if start_timestamp_min is not None:
        start_timestamp_min = _convert_to_db_timestamp(start_timestamp_min)
        stm = stm.where(JobV2.start_timestamp >= start_timestamp_min)
    if start_timestamp_max is not None:
        start_timestamp_max = _convert_to_db_timestamp(start_timestamp_max)
        stm = stm.where(JobV2.start_timestamp <= start_timestamp_max)
    if end_timestamp_min is not None:
        end_timestamp_min = _convert_to_db_timestamp(end_timestamp_min)
        stm = stm.where(JobV2.end_timestamp >= end_timestamp_min)
    if end_timestamp_max is not None:
        end_timestamp_max = _convert_to_db_timestamp(end_timestamp_max)
        stm = stm.where(JobV2.end_timestamp <= end_timestamp_max)

    res = await db.execute(stm)
    job_list = res.scalars().all()
    await db.close()
    if not log:
        for job in job_list:
            setattr(job, "log", None)

    return job_list


@router_admin_v2.get("/job/{job_id}/", response_model=JobReadV2)
async def view_single_job(
    job_id: int = None,
    show_tmp_logs: bool = False,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> JobReadV2:

    job = await db.get(JobV2, job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found",
        )
    await db.close()

    if show_tmp_logs and (job.status == JobStatusTypeV2.SUBMITTED):
        try:
            with open(f"{job.working_dir}/{WORKFLOW_LOG_FILENAME}", "r") as f:
                job.log = f.read()
        except FileNotFoundError:
            pass

    return job


@router_admin_v2.patch(
    "/job/{job_id}/",
    response_model=JobReadV2,
)
async def update_job(
    job_update: JobUpdateV2,
    job_id: int,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[JobReadV2]:
    """
    Change the status of an existing job.

    This endpoint is only open to superusers, and it does not apply
    project-based access-control to jobs.
    """
    job = await db.get(JobV2, job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found",
        )

    if job_update.status != JobStatusTypeV2.FAILED:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Cannot set job status to {job_update.status}",
        )

    setattr(job, "status", job_update.status)
    setattr(job, "end_timestamp", get_timestamp())
    await db.commit()
    await db.refresh(job)
    await db.close()
    return job


@router_admin_v2.get("/job/{job_id}/stop/", status_code=202)
async def stop_job(
    job_id: int,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> Response:
    """
    Stop execution of a workflow job.
    """

    _check_shutdown_is_supported()

    job = await db.get(JobV2, job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found",
        )

    _write_shutdown_file(job=job)

    return Response(status_code=status.HTTP_202_ACCEPTED)


@router_admin_v2.get(
    "/job/{job_id}/download/",
    response_class=StreamingResponse,
)
async def download_job_logs(
    job_id: int,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> StreamingResponse:
    """
    Download job folder
    """
    # Get job from DB
    job = await db.get(JobV2, job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found",
        )
    # Create and return byte stream for zipped log folder
    PREFIX_ZIP = Path(job.working_dir).name
    zip_filename = f"{PREFIX_ZIP}_archive.zip"
    return StreamingResponse(
        _zip_folder_to_byte_stream_iterator(folder=job.working_dir),
        media_type="application/x-zip-compressed",
        headers={"Content-Disposition": f"attachment;filename={zip_filename}"},
    )


class TaskV2Minimal(BaseModel):

    id: int
    name: str
    type: str
    command_non_parallel: Optional[str]
    command_parallel: Optional[str]
    source: str
    owner: Optional[str]
    version: Optional[str]


class ProjectUser(BaseModel):

    id: int
    email: EmailStr


class TaskV2Relationship(BaseModel):

    workflow_id: int
    workflow_name: str
    project_id: int
    project_name: str
    project_users: list[ProjectUser] = Field(default_factory=list)


class TaskV2Info(BaseModel):

    task: TaskV2Minimal
    relationships: list[TaskV2Relationship]


@router_admin_v2.get("/task/", response_model=list[TaskV2Info])
async def query_tasks(
    id: Optional[int] = None,
    source: Optional[str] = None,
    version: Optional[str] = None,
    name: Optional[str] = None,
    owner: Optional[str] = None,
    kind: Optional[Literal["common", "users"]] = None,
    max_number_of_results: int = 25,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> list[TaskV2Info]:
    """
    Query `TaskV2` table and get informations about related items
    (WorkflowV2s and ProjectV2s)

    Args:
        id: If not `None`, query for matching `task.id`.
        source: If not `None`, query for contained case insensitive
            `task.source`.
        version: If not `None`, query for matching `task.version`.
        name: If not `None`, query for contained case insensitive `task.name`.
        owner: If not `None`, query for matching `task.owner`.
        kind: If not `None`, query for TaskV2s that have (`users`) or don't
            have (`common`) a `task.owner`.
        max_number_of_results: The maximum length of the response.
    """

    stm = select(TaskV2)

    if id is not None:
        stm = stm.where(TaskV2.id == id)
    if source is not None:
        stm = stm.where(TaskV2.source.icontains(source))
    if version is not None:
        stm = stm.where(TaskV2.version == version)
    if name is not None:
        stm = stm.where(TaskV2.name.icontains(name))
    if owner is not None:
        stm = stm.where(TaskV2.owner == owner)

    if kind == "common":
        stm = stm.where(TaskV2.owner == None)  # noqa E711
    elif kind == "users":
        stm = stm.where(TaskV2.owner != None)  # noqa E711

    res = await db.execute(stm)
    task_list = res.scalars().all()
    if len(task_list) > max_number_of_results:
        await db.close()
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Too many Tasks ({len(task_list)} > {max_number_of_results})."
                " Please add more query filters."
            ),
        )

    task_info_list = []

    for task in task_list:
        stm = (
            select(WorkflowV2)
            .join(WorkflowTaskV2)
            .where(WorkflowTaskV2.workflow_id == WorkflowV2.id)
            .where(WorkflowTaskV2.task_id == task.id)
        )
        res = await db.execute(stm)
        wf_list = res.scalars().all()

        task_info_list.append(
            dict(
                task=task.model_dump(),
                relationships=[
                    dict(
                        workflow_id=workflow.id,
                        workflow_name=workflow.name,
                        project_id=workflow.project.id,
                        project_name=workflow.project.name,
                        project_users=[
                            dict(id=user.id, email=user.email)
                            for user in workflow.project.user_list
                        ],
                    )
                    for workflow in wf_list
                ],
            )
        )

    return task_info_list


@router_admin_v2.get(
    "/task-group/{task_group_id}/", response_model=TaskGroupReadV2
)
async def query_task_group(
    task_group_id: int,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> TaskGroupReadV2:

    task_group = await db.get(TaskGroupV2, task_group_id)
    if task_group is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"TaskGroup {task_group_id} not found",
        )
    return task_group


@router_admin_v2.get("/task-group/", response_model=list[TaskGroupReadV2])
async def query_task_group_list(
    user_id: Optional[int] = None,
    user_group_id: Optional[int] = None,
    private: Optional[bool] = None,
    active: Optional[bool] = None,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> list[TaskGroupReadV2]:

    stm = select(TaskGroupV2)

    if user_group_id is not None and private is True:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Cannot set `user_group_id` with {private=}",
        )
    if user_id is not None:
        stm = stm.where(TaskGroupV2.user_id == user_id)
    if user_group_id is not None:
        stm = stm.where(TaskGroupV2.user_group_id == user_group_id)
    if private is not None:
        if private is True:
            stm = stm.where(is_(TaskGroupV2.user_group_id, None))
        else:
            stm = stm.where(is_not(TaskGroupV2.user_group_id, None))
    if active is not None:
        if active is True:
            stm = stm.where(is_(TaskGroupV2.active, True))
        else:
            stm = stm.where(is_(TaskGroupV2.active, False))

    res = await db.execute(stm)
    task_groups_list = res.scalars().all()
    return task_groups_list


@router_admin_v2.patch(
    "/task-group/{task_group_id}/", response_model=TaskGroupReadV2
)
async def patch_task_group(
    task_group_id: int,
    task_group_update: TaskGroupUpdateV2,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> list[TaskGroupReadV2]:
    task_group = await db.get(TaskGroupV2, task_group_id)
    if task_group is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"TaskGroupV2 {task_group_id} not found",
        )

    for key, value in task_group_update.dict(exclude_unset=True).items():
        if (key == "user_group_id") and (value is not None):
            await _verify_user_belongs_to_group(
                user_id=user.id, user_group_id=value, db=db
            )
        setattr(task_group, key, value)

    db.add(task_group)
    await db.commit()
    await db.refresh(task_group)
    return task_group


@router_admin_v2.delete("/task-group/{task_group_id}/", status_code=204)
async def delete_task_group(
    task_group_id: int,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
):
    task_group = await db.get(TaskGroupV2, task_group_id)
    if task_group is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"TaskGroupV2 {task_group_id} not found",
        )

    stm = select(WorkflowTaskV2).where(
        WorkflowTaskV2.task_id.in_({task.id for task in task_group.task_list})
    )
    res = await db.execute(stm)
    workflow_tasks = res.scalars().all()
    if workflow_tasks != []:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"TaskV2 {workflow_tasks[0].task_id} is still in use",
        )

    await db.delete(task_group)
    await db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)
