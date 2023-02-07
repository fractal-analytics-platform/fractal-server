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
from typing import Tuple

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
from ...models import Task
from ...models import Workflow
from ...models import WorkflowCreate
from ...models import WorkflowExport
from ...models import WorkflowImport
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
) -> Tuple[WorkflowTask, Workflow]:
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


# Workflow endpoints ("/{workflow_id}")


@router.delete("/{_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_workflow(
    _id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Response:
    """
    Delte a workflow
    """

    workflow = await _get_workflow_check_owner(
        workflow_id=_id, user_id=user.id, db=db
    )

    await db.delete(workflow)
    await db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch("/{_id}", response_model=WorkflowRead)
async def patch_workflow(
    _id: int,
    patch: WorkflowUpdate,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[WorkflowRead]:
    """
    Edit a workflow
    """

    workflow = await _get_workflow_check_owner(
        workflow_id=_id, user_id=user.id, db=db
    )

    for key, value in patch.dict(exclude_unset=True).items():
        setattr(workflow, key, value)
    await db.commit()
    await db.refresh(workflow)
    return workflow


@router.get("/{_id}", response_model=WorkflowRead)
async def get_workflow(
    _id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[WorkflowRead]:
    """
    Get info on an existing workflow
    """

    workflow = await _get_workflow_check_owner(
        workflow_id=_id, user_id=user.id, db=db
    )

    return workflow


@router.post(
    "/{_id}/add-task/",
    response_model=WorkflowTaskRead,
    status_code=status.HTTP_201_CREATED,
)
async def add_task_to_workflow(
    _id: int,
    new_task: WorkflowTaskCreate,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[WorkflowTaskRead]:
    """
    Add a WorkflowTask to a Workflow
    """

    workflow = await _get_workflow_check_owner(
        workflow_id=_id, user_id=user.id, db=db
    )
    async with db:
        workflow_task = await workflow.insert_task(
            **new_task.dict(exclude={"workflow_id"}),
            db=db,
        )

    return workflow_task


# WorkflowTask endpoints ("/{workflow_id}/../{workflow_task_id}"


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
    """
    Edit a WorkflowTask of a Workflow
    """

    db_workflow_task, db_workflow = await _get_workflow_task_check_owner(
        workflow_task_id=workflow_task_id,
        workflow_id=_id,
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
    "/{_id}/rm-task/{workflow_task_id}", status_code=status.HTTP_204_NO_CONTENT
)
async def delete_task_from_workflow(
    _id: int,
    workflow_task_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Response:
    """
    Delete a WorkflowTask of a Workflow
    """

    db_workflow_task, db_workflow = await _get_workflow_task_check_owner(
        workflow_task_id=workflow_task_id,
        workflow_id=_id,
        user_id=user.id,
        db=db,
    )

    await db.delete(db_workflow_task)
    await db.commit()

    await db.refresh(db_workflow)
    db_workflow.task_list.reorder()
    await db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    "/import",
    response_model=WorkflowRead,
    status_code=status.HTTP_201_CREATED,
)
async def import_workflow_into_project(
    workflow: WorkflowImport,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[WorkflowRead]:
    """
    Import an existing workflow into a project
    """

    project_id = workflow.project_id

    # (0) Check permission to act on this project
    await _get_project_check_owner(
        project_id=project_id,
        user_id=user.id,
        db=db,
    )

    # (1) Check there is no workflow with same (name, project_id)
    # FIXME: this should be a function
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

    # (2) Check that all required tasks are available
    wf_tasks = workflow.task_list
    tasks = [wf_task.task for wf_task in wf_tasks]
    # FIXME: by now we go through the pair (source, name), but maybe we should
    # put all together - see issue #293.
    sourcename_to_id = {}
    for task in tasks:
        source = task.source
        name = task.name
        if not (source, name) in sourcename_to_id.keys():
            stm = (
                select(Task)
                .where(Task.name == name)
                .where(Task.source == source)
            )
            res = await db.execute(stm)
            current_task = res.scalars().all()
            if not len(current_task) == 1:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=(
                        f"Found {len(current_task)} tasks" f"with {source=}."
                    ),
                )
            sourcename_to_id[(source, name)] = current_task[0].id

    # (3) Create Workflow
    workflow_create = WorkflowCreate(**workflow.dict(exclude_none=True))
    db_workflow = Workflow.from_orm(workflow_create)
    db.add(db_workflow)
    await db.commit()
    await db.refresh(db_workflow)

    # (4) Insert tasks
    async with db:
        for ind, wf_task in enumerate(wf_tasks):
            # Identify task_id
            source = wf_task.task.source
            name = wf_task.task.name
            task_id = sourcename_to_id[(source, name)]
            # Prepare new_task
            new_wf_task = WorkflowTaskCreate(
                **wf_task.dict(exclude_none=True),
                workflow_id=db_workflow.id,
                task_id=task_id,
            )
            # Insert task
            await db_workflow.insert_task(
                **new_wf_task.dict(exclude={"workflow_id"}),
                db=db,
            )

    return db_workflow


@router.get(
    "/{workflow_id}/export",
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

    workflow_read = WorkflowExport(**workflow.__dict__)
    wf_dict = workflow_read.dict(
        exclude={"id", "project_id", "task_id", "workflow_id"},
        exclude_none=True,
    )
    for ind, wf_task in enumerate(wf_dict["task_list"]):
        wf_task.pop("task_id")
        wf_task.pop("workflow_id")
        wf_dict["task_list"][ind] = wf_task

    return wf_dict
