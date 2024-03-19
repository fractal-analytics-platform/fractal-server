from copy import deepcopy
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status

from ....db import AsyncSession
from ....db import get_async_db
from ....models.v1 import Task
from ....models.v2 import TaskV2
from ....schemas.v2 import WorkflowTaskCreateV2
from ....schemas.v2 import WorkflowTaskReadV2
from ....schemas.v2 import WorkflowTaskUpdateV2
from ....security import current_active_user
from ....security import User
from ._aux_functions import _get_workflow_check_owner
from ._aux_functions import _get_workflow_task_check_owner
from ._aux_functions import _workflow_insert_task

router = APIRouter()


@router.post(
    "/project/{project_id}/workflow/{workflow_id}/wftask/",
    response_model=WorkflowTaskReadV2,
    status_code=status.HTTP_201_CREATED,
)
async def create_workflowtask(
    project_id: int,
    workflow_id: int,
    new_task: WorkflowTaskCreateV2,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[WorkflowTaskReadV2]:
    """
    Add a WorkflowTask to a Workflow
    """

    workflow = await _get_workflow_check_owner(
        project_id=project_id, workflow_id=workflow_id, user_id=user.id, db=db
    )

    is_v2 = new_task.task_v2_id is not None
    task_id = new_task.task_v2_id if is_v2 else new_task.task_v1_id

    # Check that task exists
    if is_v2:
        task = await db.get(TaskV2, task_id)
    else:
        task = await db.get(Task, task_id)
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task{'V2' if is_v2 else ''} {task_id} not found.",
        )

    async with db:
        workflow_task = await _workflow_insert_task(
            workflow_id=workflow.id,
            is_v2=is_v2,
            task_id=task_id,
            order=new_task.order,
            meta=new_task.meta,
            args=new_task.args,
            attribute_filters=new_task.attribute_filters,
            flag_filters=new_task.flag_filters,
            db=db,
        )

    await db.close()

    return workflow_task


@router.get(
    "/project/{project_id}/workflow/{workflow_id}/wftask/{workflow_task_id}/",
    response_model=WorkflowTaskReadV2,
)
async def read_workflowtask(
    project_id: int,
    workflow_id: int,
    workflow_task_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
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
    "/project/{project_id}/workflow/{workflow_id}/wftask/{workflow_task_id}/",
    response_model=WorkflowTaskReadV2,
)
async def update_workflowtask(
    project_id: int,
    workflow_id: int,
    workflow_task_id: int,
    workflow_task_update: WorkflowTaskUpdateV2,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[WorkflowTaskReadV2]:
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
    "/project/{project_id}/workflow/{workflow_id}/wftask/{workflow_task_id}/",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_workflowtask(
    project_id: int,
    workflow_id: int,
    workflow_task_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
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
