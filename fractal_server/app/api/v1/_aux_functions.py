"""
Auxiliary functions to get object from the database or perform simple checks
"""
import asyncio
from typing import Union

from fastapi import Depends
from fastapi import HTTPException
from fastapi import status
from pydantic import UUID4
from sqlmodel import select

from ...db import AsyncSession
from ...db import get_db
from ...models import Dataset
from ...models import LinkUserProject
from ...models import Project
from ...models import Workflow
from ...models import WorkflowTask


async def _get_workflow_check_owner(
    *,
    workflow_id: int,
    project_id: int,
    user_id: UUID4,
    db: AsyncSession = Depends(get_db),
) -> Workflow:
    """
    Check that user is a member of a workflow's project and return

    Raises:
        HTTPException(status_code=403_FORBIDDEN): If the user is not a
                                                  member of the project
        HTTPException(status_code=404_NOT_FOUND): If the project or workflow do
                                                  not exist
    """

    # Access control for project
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

    workflow = await db.get(Workflow, workflow_id)
    if not workflow:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Workflow not found"
        )
    if workflow.project_id != project.id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Error: {workflow.project_id=} differs from {project_id=}"
            ),
        )

    return workflow


async def _get_workflow_task_check_owner(
    *,
    project_id: int,
    workflow_id: int,
    workflow_task_id: int,
    user_id: UUID4,
    db: AsyncSession = Depends(get_db),
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
    workflow = await _get_workflow_check_owner(  # noqa: F841
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
    db: AsyncSession = Depends(get_db),
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


async def _get_project_check_owner(
    *,
    project_id: int,
    user_id: UUID4,
    db: AsyncSession,
) -> Project:
    """
    Check that user is a member of project and return

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


async def _get_dataset_check_owner(
    *,
    project_id: int,
    dataset_id: int,
    user_id: UUID4,
    db: AsyncSession,
) -> dict[str, Union[Dataset, Project]]:
    """
    Check that user is a member of project and return

    Raises:
        HTTPException(status_code=403_FORBIDDEN): If the user is not a
                                                         member of the project
        HTTPException(status_code=404_NOT_FOUND): If the dataset or project do
                                                  not exist
    """
    project, dataset, link_user_project = await asyncio.gather(
        db.get(Project, project_id),
        db.get(Dataset, dataset_id),
        db.get(LinkUserProject, (project_id, user_id)),
    )

    # Access control for project
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Project not found"
        )
    if not link_user_project:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Not allowed on project {project_id}",
        )
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
