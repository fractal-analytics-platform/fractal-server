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
from sqlmodel import select

from ....config import get_settings
from ....syringe import Inject
from ....utils import get_timestamp
from ...db import AsyncSession
from ...db import get_async_db
from ...models.security import UserOAuth as User
from ...models.v1 import Task
from ...models.v2 import JobV2
from ...models.v2 import ProjectV2
from ...models.v2 import TaskV2
from ...models.v2 import WorkflowTaskV2
from ...models.v2 import WorkflowV2
from ...runner.filenames import WORKFLOW_LOG_FILENAME
from ...schemas.v2 import JobReadV2
from ...schemas.v2 import JobStatusTypeV2
from ...schemas.v2 import JobUpdateV2
from ...schemas.v2 import ProjectReadV2
from ...security import current_active_superuser
from ..aux._job import _write_shutdown_file
from ..aux._job import _zip_folder_to_byte_stream
from ..aux._runner import _check_shutdown_is_supported

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
    user: User = Depends(current_active_superuser),
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
        stm = stm.where(ProjectV2.user_list.any(User.id == user_id))

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
    user: User = Depends(current_active_superuser),
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
            ProjectV2.user_list.any(User.id == user_id)
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
    user: User = Depends(current_active_superuser),
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
    user: User = Depends(current_active_superuser),
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
    user: User = Depends(current_active_superuser),
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
    user: User = Depends(current_active_superuser),
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
    byte_stream = _zip_folder_to_byte_stream(
        folder=job.working_dir, zip_filename=zip_filename
    )
    return StreamingResponse(
        iter([byte_stream.getvalue()]),
        media_type="application/x-zip-compressed",
        headers={"Content-Disposition": f"attachment;filename={zip_filename}"},
    )


class TaskCompatibility(BaseModel):
    is_v2_compatible: bool


@router_admin_v2.patch(
    "/task-v1/{task_id}/",
    status_code=status.HTTP_200_OK,
)
async def flag_task_v1_as_v2_compatible(
    task_id: int,
    compatibility: TaskCompatibility,
    user: User = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> Response:

    task = await db.get(Task, task_id)
    if task is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task {task_id} not found",
        )

    task.is_v2_compatible = compatibility.is_v2_compatible
    await db.commit()
    await db.close()

    return Response(status_code=status.HTTP_200_OK)


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
    user: User = Depends(current_active_superuser),
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
