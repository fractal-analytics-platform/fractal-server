# Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
# University of Zurich
#
# Original author(s):
# Jacopo Nespolo <jacopo.nespolo@exact-lab.it>
# Marco Franzon <marco.franzon@exact-lab.it>
# Tommaso Comparin <tommaso.comparin@exact-lab.it>
#
# This file is part of Fractal and was originally developed by eXact lab S.r.l.
# <exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
# Institute for Biomedical Research and Pelkmans Lab from the University of
# Zurich.
import asyncio
from copy import deepcopy
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from pydantic import UUID4
from sqlmodel import select

from ...db import AsyncSession
from ...db import get_db
from ...models import LinkUserProject
from ...models import Project
from ...models import Workflow
from ...models import WorkflowCreate
from ...models import WorkflowExport
from ...models import WorkflowRead
from ...models import WorkflowTask
from ...models import WorkflowTaskCreate
from ...models import WorkflowTaskRead
from ...models import WorkflowTaskUpdate
from ...models import WorkflowUpdate
from ...security import current_active_user
from ...security import User
from .project import _get_project_check_owner

router = APIRouter()


async def _get_workflow_check_owner(
    *,
    workflow_id: int,
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

    workflow = await db.get(Workflow, workflow_id)
    if not workflow:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Workflow not found"
        )

    project, link_user_project = await asyncio.gather(
        db.get(Project, workflow.project_id),
        db.get(LinkUserProject, (workflow.project_id, user_id)),
    )
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Project not found"
        )
    if not link_user_project:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Not allowed on project {workflow.project_id}",
        )

    return workflow


async def _get_workflow_task_check_owner(
    *,
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

    workflow_task = await db.get(WorkflowTask, workflow_task_id)

    # If WorkflowTask is not in the db, exit
    if not workflow_task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="WorkflowTask not found",
        )

    # Access control for workflow
    workflow = await _get_workflow_check_owner(  # noqa: F841
        workflow_id=workflow_id, user_id=user_id, db=db
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
            detail=f"Workflow with {name=} and\
                    {project_id=} already in use",
        )


# Main endpoints ("/")


@router.post(
    "/", response_model=WorkflowRead, status_code=status.HTTP_201_CREATED
)
async def create_workflow(
    workflow: WorkflowCreate,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[WorkflowRead]:
    """
    Create a workflow, associate to a project
    """
    await _get_project_check_owner(
        project_id=workflow.project_id,
        user_id=user.id,
        db=db,
    )
    await _check_workflow_exists(
        name=workflow.name, project_id=workflow.project_id, db=db
    )

    db_workflow = Workflow.from_orm(workflow)
    db.add(db_workflow)
    await db.commit()
    await db.refresh(db_workflow)
    return db_workflow


# Workflow endpoints ("/{workflow_id}")


@router.delete("/{workflow_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_workflow(
    workflow_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Response:
    """
    Delte a workflow
    """

    workflow = await _get_workflow_check_owner(
        workflow_id=workflow_id, user_id=user.id, db=db
    )

    await db.delete(workflow)
    await db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch("/{workflow_id}", response_model=WorkflowRead)
async def patch_workflow(
    workflow_id: int,
    patch: WorkflowUpdate,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[WorkflowRead]:
    """
    Edit a workflow
    """
    workflow = await _get_workflow_check_owner(
        workflow_id=workflow_id, user_id=user.id, db=db
    )

    for key, value in patch.dict(exclude_unset=True).items():
        if key == "reordered_workflowtask_ids":
            current_workflowtask_ids = [
                wftask.id for wftask in workflow.task_list
            ]
            num_tasks = len(workflow.task_list)
            if len(value) != num_tasks or set(value) != set(
                current_workflowtask_ids
            ):
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=(
                        "`reordered_workflowtask_ids` must be a permutation of"
                        f" {current_workflowtask_ids} (given {value})"
                    ),
                )
            for ind_wftask in range(num_tasks):
                new_order = value.index(workflow.task_list[ind_wftask].id)
                workflow.task_list[ind_wftask].order = new_order
        else:
            setattr(workflow, key, value)
    await db.commit()
    await db.refresh(workflow)

    return workflow


@router.get("/{workflow_id}", response_model=WorkflowRead)
async def get_workflow(
    workflow_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[WorkflowRead]:
    """
    Get info on an existing workflow
    """

    workflow = await _get_workflow_check_owner(
        workflow_id=workflow_id, user_id=user.id, db=db
    )

    return workflow


@router.post(
    "/{workflow_id}/add-task/",
    response_model=WorkflowTaskRead,
    status_code=status.HTTP_201_CREATED,
)
async def add_task_to_workflow(
    workflow_id: int,
    new_task: WorkflowTaskCreate,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[WorkflowTaskRead]:
    """
    Add a WorkflowTask to a Workflow
    """

    workflow = await _get_workflow_check_owner(
        workflow_id=workflow_id, user_id=user.id, db=db
    )
    async with db:
        workflow_task = await workflow.insert_task(
            **new_task.dict(),
            db=db,
        )

    return workflow_task


# WorkflowTask endpoints ("/{workflow_id}/../{workflow_task_id}"


@router.patch(
    "/{workflow_id}/edit-task/{workflow_task_id}",
    response_model=WorkflowTaskRead,
)
async def patch_workflow_task(
    workflow_id: int,
    workflow_task_id: int,
    workflow_task_update: WorkflowTaskUpdate,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[WorkflowTaskRead]:
    """
    Edit a WorkflowTask of a Workflow
    """

    db_workflow_task, db_workflow = await _get_workflow_task_check_owner(
        workflow_task_id=workflow_task_id,
        workflow_id=workflow_id,
        user_id=user.id,
        db=db,
    )

    for key, value in workflow_task_update.dict(exclude_unset=True).items():
        if key == "args":
            current_args = deepcopy(db_workflow_task.args) or {}
            current_args.update(value)
            setattr(db_workflow_task, key, current_args)
        elif key == "meta":
            current_meta = deepcopy(db_workflow_task.meta) or {}
            current_meta.update(value)
            setattr(db_workflow_task, key, current_meta)
        else:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"patch_workflow_task endpoint cannot set {key=}",
            )

    await db.commit()
    await db.refresh(db_workflow_task)

    return db_workflow_task


@router.delete(
    "/{workflow_id}/rm-task/{workflow_task_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_task_from_workflow(
    workflow_id: int,
    workflow_task_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Response:
    """
    Delete a WorkflowTask of a Workflow
    """

    db_workflow_task, db_workflow = await _get_workflow_task_check_owner(
        workflow_task_id=workflow_task_id,
        workflow_id=workflow_id,
        user_id=user.id,
        db=db,
    )

    await db.delete(db_workflow_task)
    await db.commit()

    await db.refresh(db_workflow)
    db_workflow.task_list.reorder()
    await db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get(
    "/{workflow_id}/export/",
    response_model=WorkflowExport,
)
async def export_worfklow(
    workflow_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[WorkflowExport]:
    """
    Export an existing workflow, after stripping all IDs
    """
    workflow = await _get_workflow_check_owner(
        workflow_id=workflow_id, user_id=user.id, db=db
    )
    return workflow
