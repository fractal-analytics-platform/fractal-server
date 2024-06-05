"""
Auxiliary functions to get object from the database or perform simple checks
"""
from typing import Any
from typing import Literal
from typing import Optional
from typing import Union

from fastapi import HTTPException
from fastapi import status
from sqlalchemy.orm.attributes import flag_modified
from sqlmodel import select
from sqlmodel.sql.expression import SelectOfScalar

from ....db import AsyncSession
from ....models.v1 import Task
from ....models.v2 import DatasetV2
from ....models.v2 import JobV2
from ....models.v2 import LinkUserProjectV2
from ....models.v2 import ProjectV2
from ....models.v2 import TaskV2
from ....models.v2 import WorkflowTaskV2
from ....models.v2 import WorkflowV2
from ....schemas.v2 import JobStatusTypeV2
from ....security import User
from fractal_server.images import Filters


async def _get_project_check_owner(
    *,
    project_id: int,
    user_id: int,
    db: AsyncSession,
) -> ProjectV2:
    """
    Check that user is a member of project and return the project.

    Args:
        project_id:
        user_id:
        db:

    Returns:
        The project object

    Raises:
        HTTPException(status_code=403_FORBIDDEN):
            If the user is not a member of the project
        HTTPException(status_code=404_NOT_FOUND):
            If the project does not exist
    """
    project = await db.get(ProjectV2, project_id)

    link_user_project = await db.get(LinkUserProjectV2, (project_id, user_id))
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Project not found"
        )
    if not link_user_project:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Not allowed on project {project_id}",
        )

    return project


async def _get_workflow_check_owner(
    *,
    workflow_id: int,
    project_id: int,
    user_id: int,
    db: AsyncSession,
) -> WorkflowV2:
    """
    Get a workflow and a project, after access control on the project.

    Args:
        workflow_id:
        project_id:
        user_id:
        db:

    Returns:
        The workflow object.

    Raises:
        HTTPException(status_code=404_NOT_FOUND):
            If the workflow does not exist
        HTTPException(status_code=422_UNPROCESSABLE_ENTITY):
            If the workflow is not associated to the project
    """

    # Access control for project
    project = await _get_project_check_owner(
        project_id=project_id, user_id=user_id, db=db
    )

    # Get workflow
    # (See issue 1087 for 'populate_existing=True')
    workflow = await db.get(WorkflowV2, workflow_id, populate_existing=True)

    if not workflow:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Workflow not found"
        )
    if workflow.project_id != project.id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(f"Invalid {project_id=} for {workflow_id=}."),
        )

    return workflow


async def _get_workflow_task_check_owner(
    *,
    project_id: int,
    workflow_id: int,
    workflow_task_id: int,
    user_id: int,
    db: AsyncSession,
) -> tuple[WorkflowTaskV2, WorkflowV2]:
    """
    Check that user has access to Workflow and WorkflowTask.

    Args:
        project_id:
        workflow_id:
        workflow_task_id:
        user_id:
        db:

    Returns:
        Tuple of WorkflowTask and Workflow objects.

    Raises:
        HTTPException(status_code=404_NOT_FOUND):
            If the WorkflowTask does not exist
        HTTPException(status_code=422_UNPROCESSABLE_ENTITY):
            If the WorkflowTask is not associated to the Workflow
    """

    # Access control for workflow
    workflow = await _get_workflow_check_owner(
        workflow_id=workflow_id,
        project_id=project_id,
        user_id=user_id,
        db=db,
    )

    # If WorkflowTask is not in the db, exit
    workflow_task = await db.get(WorkflowTaskV2, workflow_task_id)
    if not workflow_task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="WorkflowTask not found",
        )

    # If WorkflowTask is not part of the expected Workflow, exit
    if workflow_id != workflow_task.workflow_id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid {workflow_id=} for {workflow_task_id=}",
        )

    return workflow_task, workflow


async def _check_workflow_exists(
    *,
    name: str,
    project_id: int,
    db: AsyncSession,
) -> None:
    """
    Check that no other workflow of this project has the same name.

    Args:
        name: Workflow name
        project_id: Project ID
        db:

    Raises:
        HTTPException(status_code=422_UNPROCESSABLE_ENTITY):
            If such a workflow already exists
    """
    stm = (
        select(WorkflowV2)
        .where(WorkflowV2.name == name)
        .where(WorkflowV2.project_id == project_id)
    )
    res = await db.execute(stm)
    if res.scalars().all():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Workflow with {name=} and {project_id=} already exists.",
        )


async def _check_project_exists(
    *,
    project_name: str,
    user_id: int,
    db: AsyncSession,
) -> None:
    """
    Check that no other project with this name exists for this user.

    Args:
        project_name: Project name
        user_id: User ID
        db:

    Raises:
        HTTPException(status_code=422_UNPROCESSABLE_ENTITY):
            If such a project already exists
    """
    stm = (
        select(ProjectV2)
        .join(LinkUserProjectV2)
        .where(ProjectV2.name == project_name)
        .where(LinkUserProjectV2.user_id == user_id)
    )
    res = await db.execute(stm)
    if res.scalars().all():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Project name ({project_name}) already in use",
        )


async def _get_dataset_check_owner(
    *,
    project_id: int,
    dataset_id: int,
    user_id: int,
    db: AsyncSession,
) -> dict[Literal["dataset", "project"], Union[DatasetV2, ProjectV2]]:
    """
    Get a dataset and a project, after access control on the project

    Args:
        project_id:
        dataset_id:
        user_id:
        db:

    Returns:
        Dictionary with the dataset and project objects (keys: `dataset`,
            `project`).

    Raises:
        HTTPException(status_code=422_UNPROCESSABLE_ENTITY):
            If the dataset is not associated to the project
    """
    # Access control for project
    project = await _get_project_check_owner(
        project_id=project_id, user_id=user_id, db=db
    )

    # Get dataset
    # (See issue 1087 for 'populate_existing=True')
    dataset = await db.get(DatasetV2, dataset_id, populate_existing=True)

    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found"
        )
    if dataset.project_id != project_id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid {project_id=} for {dataset_id=}",
        )

    return dict(dataset=dataset, project=project)


async def _get_job_check_owner(
    *,
    project_id: int,
    job_id: int,
    user_id: int,
    db: AsyncSession,
) -> dict[Literal["job", "project"], Union[JobV2, ProjectV2]]:
    """
    Get a job and a project, after access control on the project

    Args:
        project_id:
        job_id:
        user_id:
        db:

    Returns:
        Dictionary with the job and project objects (keys: `job`,
            `project`).

    Raises:
        HTTPException(status_code=422_UNPROCESSABLE_ENTITY):
            If the job is not associated to the project
    """
    # Access control for project
    project = await _get_project_check_owner(
        project_id=project_id,
        user_id=user_id,
        db=db,
    )
    # Get dataset
    job = await db.get(JobV2, job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Job not found"
        )
    if job.project_id != project_id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid {project_id=} for {job_id=}",
        )
    return dict(job=job, project=project)


async def _get_task_check_owner(
    *,
    task_id: int,
    user: User,
    db: AsyncSession,
) -> TaskV2:
    """
    Get a task, after access control.

    This check constitutes a preliminary version of access control:
    if the current user is not a superuser and differs from the task owner
    (including when `owner is None`), we raise an 403 HTTP Exception.

    Args:
        task_id:
        user:
        db:

    Returns:
        The task object.

    Raises:
        HTTPException(status_code=404_NOT_FOUND):
            If the task does not exist
        HTTPException(status_code=403_FORBIDDEN):
            If the user does not have rights to edit this task.
    """
    task = await db.get(TaskV2, task_id)
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"TaskV2 {task_id} not found.",
        )

    if not user.is_superuser:
        if task.owner is None:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    "Only a superuser can modify a TaskV2 with `owner=None`."
                ),
            )
        else:
            owner = user.username or user.slurm_user
            if owner != task.owner:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=(
                        f"Current user ({owner}) cannot modify TaskV2 "
                        f"{task.id} with different owner ({task.owner})."
                    ),
                )
    return task


def _get_submitted_jobs_statement() -> SelectOfScalar:
    """
    Returns:
        A sqlmodel statement that selects all `Job`s with
        `Job.status` equal to `submitted`.
    """
    stm = select(JobV2).where(JobV2.status == JobStatusTypeV2.SUBMITTED)
    return stm


async def _workflow_insert_task(
    *,
    workflow_id: int,
    task_id: int,
    is_legacy_task: bool = False,
    order: Optional[int] = None,
    meta_parallel: Optional[dict[str, Any]] = None,
    meta_non_parallel: Optional[dict[str, Any]] = None,
    args_non_parallel: Optional[dict[str, Any]] = None,
    args_parallel: Optional[dict[str, Any]] = None,
    input_filters: Optional[Filters] = None,
    db: AsyncSession,
) -> WorkflowTaskV2:
    """
    Insert a new WorkflowTask into Workflow.task_list

    Args:
        workflow_id:
        task_id:
        is_legacy_task:
        order:
        meta_parallel:
        meta_non_parallel:
        args_non_parallel:
        args_parallel:
        input_filters:
        db:
    """
    db_workflow = await db.get(WorkflowV2, workflow_id)
    if db_workflow is None:
        raise ValueError(f"Workflow {workflow_id} does not exist")

    if order is None:
        order = len(db_workflow.task_list)

    # Get task from db, and extract default arguments via a Task property
    # method
    if is_legacy_task is True:
        db_task = await db.get(Task, task_id)
        if db_task is None:
            raise ValueError(f"Task {task_id} not found.")
        task_type = "parallel"

        final_args_parallel = db_task.default_args_from_args_schema.copy()
        final_args_non_parallel = {}
        final_meta_parallel = (db_task.meta or {}).copy()
        final_meta_non_parallel = {}

    else:
        db_task = await db.get(TaskV2, task_id)
        if db_task is None:
            raise ValueError(f"TaskV2 {task_id} not found.")
        task_type = db_task.type

        final_args_non_parallel = (
            db_task.default_args_non_parallel_from_args_schema.copy()
        )
        final_args_parallel = (
            db_task.default_args_parallel_from_args_schema.copy()
        )
        final_meta_parallel = (db_task.meta_parallel or {}).copy()
        final_meta_non_parallel = (db_task.meta_non_parallel or {}).copy()

    # Combine arg_parallel
    if args_parallel is not None:
        for k, v in args_parallel.items():
            final_args_parallel[k] = v
    if final_args_parallel == {}:
        final_args_parallel = None
    # Combine arg_non_parallel
    if args_non_parallel is not None:
        for k, v in args_non_parallel.items():
            final_args_non_parallel[k] = v
    if final_args_non_parallel == {}:
        final_args_non_parallel = None

    # Combine meta_parallel (higher priority)
    # and db_task.meta_parallel (lower priority)
    final_meta_parallel.update(meta_parallel or {})
    if final_meta_parallel == {}:
        final_meta_parallel = None
    # Combine meta_non_parallel (higher priority)
    # and db_task.meta_non_parallel (lower priority)
    final_meta_non_parallel.update(meta_non_parallel or {})
    if final_meta_non_parallel == {}:
        final_meta_non_parallel = None

    # Prepare input_filters attribute
    if input_filters is None:
        input_filters_kwarg = {}
    else:
        input_filters_kwarg = dict(input_filters=input_filters)

    # Create DB entry
    wf_task = WorkflowTaskV2(
        task_type=task_type,
        is_legacy_task=is_legacy_task,
        task_id=(task_id if not is_legacy_task else None),
        task_legacy_id=(task_id if is_legacy_task else None),
        args_non_parallel=final_args_non_parallel,
        args_parallel=final_args_parallel,
        meta_parallel=final_meta_parallel,
        meta_non_parallel=final_meta_non_parallel,
        **input_filters_kwarg,
    )
    db_workflow.task_list.insert(order, wf_task)
    db_workflow.task_list.reorder()  # type: ignore
    flag_modified(db_workflow, "task_list")
    await db.commit()

    # See issue 1087 for 'populate_existing=True'
    wf_task = await db.get(WorkflowTaskV2, wf_task.id, populate_existing=True)

    return wf_task


async def clean_app_job_list_v2(
    db: AsyncSession, jobs_list: list[int]
) -> list[int]:
    """
    Remove from a job list all jobs with status different from submitted.

    Args:
        db: Async database session
        jobs_list: List of job IDs currently associated to the app.

    Return:
        List of IDs for submitted jobs.
    """
    stmt = select(JobV2).where(JobV2.id.in_(jobs_list))
    result = await db.execute(stmt)
    db_jobs_list = result.scalars().all()
    submitted_job_ids = [
        job.id
        for job in db_jobs_list
        if job.status == JobStatusTypeV2.SUBMITTED
    ]
    return submitted_job_ids
