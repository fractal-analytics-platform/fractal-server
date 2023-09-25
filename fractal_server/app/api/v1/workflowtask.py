# Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
# University of Zurich
#
# Original author(s):
# Jacopo Nespolo <jacopo.nespolo@exact-lab.it>
# Marco Franzon <marco.franzon@exact-lab.it>
# Tommaso Comparin <tommaso.comparin@exact-lab.it>
# Yuri Chiucconi <yuri.chiucconi@exact-lab.it>
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

from ...db import AsyncSession
from ...db import get_db
from ...models import Task
from ...schemas import WorkflowTaskCreate
from ...schemas import WorkflowTaskRead
from ...schemas import WorkflowTaskUpdate
from ...security import current_active_user
from ...security import User
from ._aux_functions import _get_workflow_check_owner
from ._aux_functions import _get_workflow_task_check_owner

router = APIRouter()


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

    # Check that task exists
    task = await db.get(Task, task_id)
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task {task_id} not found.",
        )

    async with db:
        workflow_task = await workflow.insert_task(
            **new_task.dict(),
            task_id=task_id,
            db=db,
        )

    await db.close()
    return workflow_task


@router.get(
    "/project/{project_id}/workflow/{workflow_id}/wftask/{workflow_task_id}",
    response_model=WorkflowTaskRead,
)
async def read_workflowtask(
    project_id: int,
    workflow_id: int,
    workflow_task_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
):
    workflow_task, _ = await _get_workflow_task_check_owner(
        project_id=project_id,
        workflow_task_id=workflow_task_id,
        workflow_id=workflow_id,
        user_id=user.id,
        db=db,
    )
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

            # Get default arguments via a Task property method
            default_args = deepcopy(
                db_workflow_task.task.default_args_from_args_schema
            )
            # Override default_args with args value items
            actual_args = default_args.copy()
            if value is not None:
                for k, v in value.items():
                    actual_args[k] = v
            if not actual_args:
                actual_args = None
            setattr(db_workflow_task, key, actual_args)
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
