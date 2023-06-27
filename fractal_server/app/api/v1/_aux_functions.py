"""
Auxiliary functions to get object from the database or perform simple checks
"""
import asyncio
from typing import Union

from fastapi import HTTPException
from fastapi import status
from sqlmodel import select

from ...db import AsyncSession
from ...models import ApplyWorkflow
from ...models import Dataset
from ...models import LinkUserProject
from ...models import Project
from ...models import Task
from ...models import Workflow
from ...models import WorkflowTask
from ...security import User


async def _get_project_check_owner(
    *,
    project_id: int,
    user_id: int,
    db: AsyncSession,
) -> Project:
    """
    Check that user is a member of project and return the project

    This function is at the basis of most access-control checks.

    Raises:
        HTTPException(status_code=403_FORBIDDEN): If the user is not a
                                                  member of the project
        HTTPException(status_code=404_NOT_FOUND): If the project does not
                                                  exist
    """
    project, link_user_project = await asyncio.gather(
        db.get(Project, project_id),
        db.get(LinkUserProject, (project_id, user_id)),
    )
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
    Get a workflow and a project, after access control on the project
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
    Check that user has rights to access a Workflow and a WorkflowTask and
    return the WorkflowTask

    Raises:
        HTTPException(status_code=404_NOT_FOUND): If the WorkflowTask does not
                                                  exist
        HTTPException(status_code=422_UNPROCESSABLE_ENTITY): If the
                                                             WorkflowTask is
                                                             not associated to
                                                             the Workflow
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
):
    """
    Check that there is no existing workflow for the same project and with the
    same name

    Arguments:
        name: Workflow name
        project_id: Project ID

    Raises:
        HTTPException(status_code=422_UNPROCESSABLE_ENTITY): If such a workflow
                                                             already exists
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
            detail=f"Workflow with {name=} and {project_id=} already in use",
        )


async def _get_dataset_check_owner(
    *,
    project_id: int,
    dataset_id: int,
    user_id: int,
    db: AsyncSession,
) -> dict[str, Union[Dataset, Project]]:
    """
    Get a dataset and a project, after access control on the project
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
    return dict(dataset=dataset, project=project)


async def _get_job_check_owner(
    *,
    project_id: int,
    job_id: int,
    user_id: int,
    db: AsyncSession,
) -> dict[str, Union[ApplyWorkflow, Project]]:
    """
    Get a job and a project, after access control on the project
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
    This check constitutes a preliminary version of access control:
    if the current user is not a superuser and differs from the task owner
    (including when `owner is None`), we raise an 403 HTTP Exception.
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
