from copy import deepcopy
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status

from ....db import AsyncSession
from ....db import get_async_db
from ....schemas.v2 import WorkflowTaskCreateV2
from ....schemas.v2 import WorkflowTaskReadV2
from ....schemas.v2 import WorkflowTaskUpdateV2
from ._aux_functions import _get_workflow_check_owner
from ._aux_functions import _get_workflow_task_check_owner
from ._aux_functions import _workflow_insert_task
from ._aux_functions_tasks import _get_task_read_access
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth import current_active_user

router = APIRouter()


@router.post(
    "/project/{project_id}/workflow/{workflow_id}/wftask/",
    response_model=WorkflowTaskReadV2,
    status_code=status.HTTP_201_CREATED,
)
async def create_workflowtask(
    project_id: int,
    workflow_id: int,
    task_id: int,
    new_task: WorkflowTaskCreateV2,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[WorkflowTaskReadV2]:
    """
    Add a WorkflowTask to a Workflow
    """

    workflow = await _get_workflow_check_owner(
        project_id=project_id, workflow_id=workflow_id, user_id=user.id, db=db
    )

    task = await _get_task_read_access(
        task_id=task_id, user_id=user.id, db=db, require_active=True
    )

    if task.type == "parallel":
        if (
            new_task.meta_non_parallel is not None
            or new_task.args_non_parallel is not None
        ):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    "Cannot set `WorkflowTaskV2.meta_non_parallel` or "
                    "`WorkflowTask.args_non_parallel` if the associated Task "
                    "is `parallel`."
                ),
            )
    elif task.type == "non_parallel":
        if (
            new_task.meta_parallel is not None
            or new_task.args_parallel is not None
        ):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    "Cannot set `WorkflowTaskV2.meta_parallel` or "
                    "`WorkflowTask.args_parallel` if the associated Task "
                    "is `non_parallel`."
                ),
            )

    workflow_task = await _workflow_insert_task(
        workflow_id=workflow.id,
        task_id=task_id,
        meta_non_parallel=new_task.meta_non_parallel,
        meta_parallel=new_task.meta_parallel,
        args_non_parallel=new_task.args_non_parallel,
        args_parallel=new_task.args_parallel,
        input_filters=new_task.input_filters,
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
    user: UserOAuth = Depends(current_active_user),
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
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[WorkflowTaskReadV2]:
    """
    Edit a WorkflowTask of a Workflow
    """

    db_wf_task, db_workflow = await _get_workflow_task_check_owner(
        project_id=project_id,
        workflow_task_id=workflow_task_id,
        workflow_id=workflow_id,
        user_id=user.id,
        db=db,
    )

    if db_wf_task.task_type == "parallel" and (
        workflow_task_update.args_non_parallel is not None
        or workflow_task_update.meta_non_parallel is not None
    ):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "Cannot patch `WorkflowTaskV2.args_non_parallel` or "
                "`WorkflowTask.meta_non_parallel` if the associated Task is "
                "parallel."
            ),
        )
    elif db_wf_task.task_type == "non_parallel" and (
        workflow_task_update.args_parallel is not None
        or workflow_task_update.meta_parallel is not None
    ):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "Cannot patch `WorkflowTaskV2.args_parallel` or "
                "`WorkflowTask.meta_parallel` if the associated Task is "
                "non parallel."
            ),
        )

    for key, value in workflow_task_update.dict(exclude_unset=True).items():
        if key == "args_parallel":
            # Get default arguments via a Task property method
            actual_args = deepcopy(value)
            if not actual_args:
                actual_args = None
            setattr(db_wf_task, key, actual_args)
        elif key == "args_non_parallel":
            actual_args = deepcopy(value)
            if not actual_args:
                actual_args = None
            setattr(db_wf_task, key, actual_args)
        elif key in ["meta_parallel", "meta_non_parallel", "input_filters"]:
            setattr(db_wf_task, key, value)
        else:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"patch_workflow_task endpoint cannot set {key=}",
            )

    await db.commit()
    await db.refresh(db_wf_task)
    await db.close()

    return db_wf_task


@router.delete(
    "/project/{project_id}/workflow/{workflow_id}/wftask/{workflow_task_id}/",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_workflowtask(
    project_id: int,
    workflow_id: int,
    workflow_task_id: int,
    user: UserOAuth = Depends(current_active_user),
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
