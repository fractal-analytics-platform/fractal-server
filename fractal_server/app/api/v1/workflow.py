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
from copy import deepcopy
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from sqlmodel import select

from ...db import AsyncSession
from ...db import get_db
from ...models import Task
from ...models import Workflow
from ...models import WorkflowCreate
from ...models import WorkflowExport
from ...models import WorkflowImport
from ...models import WorkflowRead
from ...models import WorkflowTaskCreate
from ...models import WorkflowTaskRead
from ...models import WorkflowTaskUpdate
from ...models import WorkflowUpdate
from ...security import current_active_user
from ...security import User
from ._aux_functions import _check_workflow_exists
from ._aux_functions import _get_project_check_owner
from ._aux_functions import _get_workflow_check_owner
from ._aux_functions import _get_workflow_task_check_owner


router = APIRouter()


@router.get(
    "/project/{project_id}/workflow/",
    response_model=list[WorkflowRead],
)
async def get_workflow_list(
    project_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[list[WorkflowRead]]:
    """
    Get list of workflows associated to the current project
    """
    await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )
    stm = select(Workflow).where(Workflow.project_id == project_id)
    res = await db.execute(stm)
    workflow_list = res.scalars().all()
    await db.close()
    return workflow_list


@router.post(
    "/project/{project_id}/workflow/",
    response_model=WorkflowRead,
    status_code=status.HTTP_201_CREATED,
)
async def create_workflow(
    project_id: int,
    workflow: WorkflowCreate,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[WorkflowRead]:
    """
    Create a workflow, associate to a project
    """
    await _get_project_check_owner(
        project_id=project_id,
        user_id=user.id,
        db=db,
    )
    await _check_workflow_exists(
        name=workflow.name, project_id=project_id, db=db
    )

    db_workflow = Workflow(project_id=project_id, **workflow.dict())
    db.add(db_workflow)
    await db.commit()
    await db.refresh(db_workflow)
    await db.close()
    return db_workflow


@router.get(
    "/project/{project_id}/workflow/{workflow_id}",
    response_model=WorkflowRead,
)
async def read_workflow(
    project_id: int,
    workflow_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[WorkflowRead]:
    """
    Get info on an existing workflow
    """

    workflow = await _get_workflow_check_owner(
        project_id=project_id, workflow_id=workflow_id, user_id=user.id, db=db
    )

    return workflow


@router.patch(
    "/project/{project_id}/workflow/{workflow_id}",
    response_model=WorkflowRead,
)
async def update_workflow(
    project_id: int,
    workflow_id: int,
    patch: WorkflowUpdate,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[WorkflowRead]:
    """
    Edit a workflow
    """
    workflow = await _get_workflow_check_owner(
        project_id=project_id, workflow_id=workflow_id, user_id=user.id, db=db
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
    await db.close()

    return workflow


@router.delete(
    "/project/{project_id}/workflow/{workflow_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_workflow(
    project_id: int,
    workflow_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Response:
    """
    Delte a workflow
    """

    workflow = await _get_workflow_check_owner(
        project_id=project_id, workflow_id=workflow_id, user_id=user.id, db=db
    )

    await db.delete(workflow)
    await db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get(
    "/project/{project_id}/workflow/{workflow_id}/export/",
    response_model=WorkflowExport,
)
async def export_worfklow(
    project_id: int,
    workflow_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[WorkflowExport]:
    """
    Export an existing workflow, after stripping all IDs
    """
    workflow = await _get_workflow_check_owner(
        project_id=project_id, workflow_id=workflow_id, user_id=user.id, db=db
    )
    await db.close()
    return workflow


@router.post(
    "/project/{project_id}/workflow/import/",
    response_model=WorkflowRead,
    status_code=status.HTTP_201_CREATED,
)
async def import_workflow(
    project_id: int,
    workflow: WorkflowImport,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[WorkflowRead]:
    """
    Import an existing workflow into a project

    Also create all required objects (i.e. Workflow and WorkflowTask's) along
    the way.
    """

    # Preliminary checks
    await _get_project_check_owner(
        project_id=project_id,
        user_id=user.id,
        db=db,
    )

    await _check_workflow_exists(
        name=workflow.name, project_id=project_id, db=db
    )

    # Check that all required tasks are available
    # NOTE: by now we go through the pair (source, name), but later on we may
    # combine them into source -- see issue #293.
    tasks = [wf_task.task for wf_task in workflow.task_list]
    sourcename_to_id = {}
    for task in tasks:
        source = task.source
        name = task.name
        if not (source, name) in sourcename_to_id.keys():
            stm = select(Task).where(Task.source == source)
            tasks_by_source = (await db.execute(stm)).scalars().all()
            if not tasks_by_source:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=(f"Found 0 tasks with {source=}."),
                )
            else:
                stm = (
                    select(Task)
                    .where(Task.source == source)
                    .where(Task.name == name)
                )
                current_task = (await db.execute(stm)).scalars().all()
                if len(current_task) != 1:
                    raise HTTPException(
                        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                        detail=(
                            f"Found {len(current_task)} tasks with "
                            f"{name =} and {source=}."
                        ),
                    )
                sourcename_to_id[(source, name)] = current_task[0].id

    # Create new Workflow (with empty task_list)
    db_workflow = Workflow(
        project_id=project_id,
        **workflow.dict(exclude_none=True, exclude={"task_list"}),
    )
    db.add(db_workflow)
    await db.commit()
    await db.refresh(db_workflow)

    # Insert tasks
    async with db:
        for _, wf_task in enumerate(workflow.task_list):
            # Identify task_id
            source = wf_task.task.source
            name = wf_task.task.name
            task_id = sourcename_to_id[(source, name)]
            # Prepare new_wf_task
            new_wf_task = WorkflowTaskCreate(
                **wf_task.dict(exclude_none=True),
            )
            # Insert task
            await db_workflow.insert_task(
                **new_wf_task.dict(),
                task_id=task_id,
                db=db,
            )

    await db.close()
    return db_workflow


@router.post(
    "/project/{project_id}/workflow/{workflow_id}/wftask/",
    response_model=WorkflowTaskRead,
    status_code=status.HTTP_201_CREATED,
)
async def create_workflowtask(
    project_id: int,
    workflow_id: int,
    task_id: int,
    new_task: WorkflowTaskCreate,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[WorkflowTaskRead]:
    """
    Add a WorkflowTask to a Workflow
    """

    workflow = await _get_workflow_check_owner(
        project_id=project_id, workflow_id=workflow_id, user_id=user.id, db=db
    )
    async with db:
        workflow_task = await workflow.insert_task(
            **new_task.dict(),
            task_id=task_id,
            db=db,
        )

    await db.close()
    return workflow_task


@router.patch(
    "/project/{project_id}/workflow/{workflow_id}/wftask/{workflow_task_id}",
    response_model=WorkflowTaskRead,
)
async def update_workflowtask(
    project_id: int,
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
        project_id=project_id,
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
    await db.close()

    return db_workflow_task


@router.delete(
    "/project/{project_id}/workflow/{workflow_id}/wftask/{workflow_task_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_workflowtask(
    project_id: int,
    workflow_id: int,
    workflow_task_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Response:
    """
    Delete a WorkflowTask of a Workflow
    """

    db_workflow_task, db_workflow = await _get_workflow_task_check_owner(
        project_id=project_id,
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
