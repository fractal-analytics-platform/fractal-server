"""
Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
University of Zurich

Original author(s):
Jacopo Nespolo <jacopo.nespolo@exact-lab.it>
Marco Franzon <marco.franzon@exact-lab.it>
Tommaso Comparin <tommaso.comparin@exact-lab.it>

This file is part of Fractal and was originally developed by eXact lab S.r.l.
<exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
Institute for Biomedical Research and Pelkmans Lab from the University of
Zurich.
"""
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
from ...models import WorkflowRead
from ...models import WorkflowTask
from ...models import WorkflowTaskCreate
from ...models import WorkflowTaskRead
from ...models import WorkflowTaskUpdate
from ...models import WorkflowUpdate
from ...security import current_active_user
from ...security import User
from .project import get_project_check_owner

router = APIRouter()


async def get_workflow_check_owner(
    *,
    workflow_id: int,
    user_id: UUID4,
    db: AsyncSession = Depends(get_db),
) -> Workflow:
    """
    Check that user is a member of a workflow's project and return

    Raise 403_FORBIDDEN if the user is not a member
    Raise 404_NOT_FOUND if the project or workflow do not exist
    """

    workflow = await db.get(Workflow, workflow_id)
    project, link_user_project = await asyncio.gather(
        db.get(Project, workflow.project_id),
        db.get(LinkUserProject, (workflow.project_id, user_id)),
    )
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Project not found"
        )
    if not workflow:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Workflow not found"
        )
    if not link_user_project:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Not allowed on project {workflow.project_id}",
        )

    return workflow


@router.post(
    "/", response_model=WorkflowRead, status_code=status.HTTP_201_CREATED
)
async def create_workflow(
    workflow: WorkflowCreate,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[WorkflowRead]:
    await get_project_check_owner(
        project_id=workflow.project_id,
        user_id=user.id,
        db=db,
    )
    # Check that there is no workflow with the same name
    # and same project_id
    stm = (
        select(Workflow)
        .where(Workflow.name == workflow.name)
        .where(Workflow.project_id == workflow.project_id)
    )
    res = await db.execute(stm)
    if res.scalars().all():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Workflow with name={workflow.name} and\
                    project_id={workflow.project_id} already in use",
        )
    db_workflow = Workflow.from_orm(workflow)
    db.add(db_workflow)
    await db.commit()
    await db.refresh(db_workflow)
    return db_workflow


@router.delete("/{_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_workflow(
    _id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Response:

    workflow = get_workflow_check_owner(workflow_id=_id, user_id=user.id)

    await db.delete(workflow)
    await db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get("/{_id}", response_model=WorkflowRead)
async def get_workflow(
    _id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[WorkflowRead]:

    workflow = get_workflow_check_owner(workflow_id=_id, user_id=user.id)

    return workflow


@router.post(
    "/{_id}/add-task/",
    response_model=WorkflowRead,
    status_code=status.HTTP_201_CREATED,
)
async def add_task_to_workflow(
    _id: int,
    new_task: WorkflowTaskCreate,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[WorkflowRead]:

    workflow = get_workflow_check_owner(workflow_id=_id, user_id=user.id)

    await workflow.insert_task(
        **new_task.dict(exclude={"workflow_id"}),
        db=db,
    )

    await db.commit()
    await db.refresh(workflow)

    return workflow


@router.patch(
    "/{_id}/edit-task/{workflow_task_id}", response_model=WorkflowTaskRead
)
async def patch_workflow_task(
    _id: int,
    workflow_task_id: int,
    workflow_task_update: WorkflowTaskUpdate,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[WorkflowTaskRead]:

    # FIXME add user-owned workflowtasks

    db_workflow = get_workflow_check_owner(  # noqa: F841
        workflow_id=_id, user_id=user.id
    )
    db_workflow_task = await db.get(WorkflowTask, workflow_task_id)

    if not db_workflow_task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Workflow Task not found",
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
    "/{_id}/rm-task/{workflow_task_id}", status_code=status.HTTP_204_NO_CONTENT
)
async def delete_task_from_workflow(
    _id: int,
    workflow_task_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Response:

    workflow = get_workflow_check_owner(workflow_id=_id, user_id=user.id)

    to_delete = await db.get(WorkflowTask, workflow_task_id)
    await db.delete(to_delete)
    await db.commit()

    await db.refresh(workflow)
    workflow.task_list.reorder()
    await db.commit()
    return


@router.patch("/{_id}", response_model=WorkflowRead)
async def patch_workflow(
    _id: int,
    patch: WorkflowUpdate,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[WorkflowRead]:

    workflow = get_workflow_check_owner(workflow_id=_id, user_id=user.id)

    for key, value in patch.dict(exclude_unset=True).items():
        setattr(workflow, key, value)
    await db.commit()
    await db.refresh(workflow)
    return workflow
