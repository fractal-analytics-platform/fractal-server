"""
Auxiliary functions to get object from the database or perform simple checks
"""
from typing import Any
from typing import Literal
from typing import Optional
from typing import Union

from fastapi import HTTPException
from fastapi import status
from sqlmodel import select
from sqlmodel.sql.expression import SelectOfScalar

from ....db import AsyncSession
from ....models.v1 import ApplyWorkflow
from ....models.v1 import Dataset
from ....models.v1 import LinkUserProject
from ....models.v1 import Project
from ....models.v1 import Task
from ....models.v1 import Workflow
from ....models.v1 import WorkflowTask
from ....schemas.v1 import JobStatusTypeV1
from ....security import User


async def _get_project_check_owner(
    *,
    project_id: int,
    user_id: int,
    db: AsyncSession,
) -> Project:
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
    project = await db.get(Project, project_id)
    link_user_project = await db.get(LinkUserProject, (project_id, user_id))
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
) -> Workflow:
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
    workflow = await db.get(Workflow, workflow_id)
    if not workflow:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Workflow not found"
        )
    if workflow.project_id != project.id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(f"Invalid {project_id=} for {workflow_id=}."),
        )

    # Refresh so that workflow.project relationship is loaded (see discussion
    # in issue #1063)
    await db.refresh(workflow)

    return workflow


async def _get_workflow_task_check_owner(
    *,
    project_id: int,
    workflow_id: int,
    workflow_task_id: int,
    user_id: int,
    db: AsyncSession,
) -> tuple[WorkflowTask, Workflow]:
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
        workflow_id=workflow_id, project_id=project_id, user_id=user_id, db=db
    )

    # If WorkflowTask is not in the db, exit
    workflow_task = await db.get(WorkflowTask, workflow_task_id)
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
        select(Workflow)
        .where(Workflow.name == name)
        .where(Workflow.project_id == project_id)
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
        select(Project)
        .join(LinkUserProject)
        .where(Project.name == project_name)
        .where(LinkUserProject.user_id == user_id)
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
) -> dict[Literal["dataset", "project"], Union[Dataset, Project]]:
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
    dataset = await db.get(Dataset, dataset_id)
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found"
        )
    if dataset.project_id != project_id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid {project_id=} for {dataset_id=}",
        )

    # Refresh so that dataset.project relationship is loaded (see discussion
    # in issue #1063)
    await db.refresh(dataset)

    return dict(dataset=dataset, project=project)


async def _get_job_check_owner(
    *,
    project_id: int,
    job_id: int,
    user_id: int,
    db: AsyncSession,
) -> dict[Literal["job", "project"], Union[ApplyWorkflow, Project]]:
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
        project_id=project_id, user_id=user_id, db=db
    )
    # Get dataset
    job = await db.get(ApplyWorkflow, job_id)
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
) -> Task:
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
    task = await db.get(Task, task_id)
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task {task_id} not found.",
        )

    if not user.is_superuser:
        if task.owner is None:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    "Only a superuser can modify a Task with `owner=None`."
                ),
            )
        else:
            owner = user.username or user.slurm_user
            if owner != task.owner:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=(
                        f"Current user ({owner}) cannot modify Task {task.id} "
                        f"with different owner ({task.owner})."
                    ),
                )
    return task


def _get_submitted_jobs_statement() -> SelectOfScalar:
    """
    Returns:
        A sqlmodel statement that selects all `ApplyWorkflow`s with
        `ApplyWorkflow.status` equal to `submitted`.
    """
    stm = select(ApplyWorkflow).where(
        ApplyWorkflow.status == JobStatusTypeV1.SUBMITTED
    )
    return stm


async def _workflow_insert_task(
    *,
    workflow_id: int,
    task_id: int,
    args: Optional[dict[str, Any]] = None,
    meta: Optional[dict[str, Any]] = None,
    order: Optional[int] = None,
    db: AsyncSession,
) -> WorkflowTask:
    """
    Insert a new WorkflowTask into Workflow.task_list

    Args:
        task_id: TBD
        args: TBD
        meta: TBD
        order: TBD
        db: TBD
    """
    db_workflow = await db.get(Workflow, workflow_id)
    if db_workflow is None:
        raise ValueError(f"Workflow {workflow_id} does not exist")

    if order is None:
        order = len(db_workflow.task_list)

    # Get task from db, and extract default arguments via a Task property
    # method
    db_task = await db.get(Task, task_id)
    if db_task is None:
        raise ValueError(f"Task {task_id} does not exist")

    default_args = db_task.default_args_from_args_schema
    # Override default_args with args
    actual_args = default_args.copy()
    if args is not None:
        for k, v in args.items():
            actual_args[k] = v
    if not actual_args:
        actual_args = None

    # Combine meta (higher priority) and db_task.meta (lower priority)
    wt_meta = (db_task.meta or {}).copy()
    wt_meta.update(meta or {})
    if not wt_meta:
        wt_meta = None

    # Create DB entry
    wf_task = WorkflowTask(task_id=task_id, args=actual_args, meta=wt_meta)
    db.add(wf_task)
    db_workflow.task_list.insert(order, wf_task)
    db_workflow.task_list.reorder()  # type: ignore
    await db.commit()
    await db.refresh(wf_task)

    return wf_task


async def clean_app_job_list_v1(
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
    stmt = select(ApplyWorkflow).where(ApplyWorkflow.id.in_(jobs_list))
    result = await db.execute(stmt)
    db_jobs_list = result.scalars().all()
    submitted_job_ids = [
        job.id
        for job in db_jobs_list
        if job.status == JobStatusTypeV1.SUBMITTED
    ]
    return submitted_job_ids
