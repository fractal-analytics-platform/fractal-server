from copy import deepcopy

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status

from ....db import AsyncSession
from ....db import get_async_db
from ._aux_functions import _get_workflow_check_owner
from ._aux_functions import _get_workflow_task_check_owner
from ._aux_functions import _workflow_has_submitted_job
from ._aux_functions import _workflow_insert_task
from ._aux_functions_tasks import _check_type_filters_compatibility
from ._aux_functions_tasks import _get_task_read_access
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth import current_active_user
from fractal_server.app.schemas.v2 import TaskType
from fractal_server.app.schemas.v2 import WorkflowTaskCreateV2
from fractal_server.app.schemas.v2 import WorkflowTaskReadV2
from fractal_server.app.schemas.v2 import WorkflowTaskUpdateV2

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
    wftask: WorkflowTaskCreateV2,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> WorkflowTaskReadV2 | None:
    """
    Add a WorkflowTask to a Workflow
    """

    workflow = await _get_workflow_check_owner(
        project_id=project_id, workflow_id=workflow_id, user_id=user.id, db=db
    )

    task = await _get_task_read_access(
        task_id=task_id, user_id=user.id, db=db, require_active=True
    )

    if task.type == TaskType.PARALLEL:
        if (
            wftask.meta_non_parallel is not None
            or wftask.args_non_parallel is not None
        ):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    "Cannot set `WorkflowTaskV2.meta_non_parallel` or "
                    "`WorkflowTask.args_non_parallel` if the associated Task "
                    "is `parallel`."
                ),
            )
    elif task.type == TaskType.NON_PARALLEL:
        if (
            wftask.meta_parallel is not None
            or wftask.args_parallel is not None
        ):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    "Cannot set `WorkflowTaskV2.meta_parallel` or "
                    "`WorkflowTask.args_parallel` if the associated Task "
                    "is `non_parallel`."
                ),
            )

    _check_type_filters_compatibility(
        task_input_types=task.input_types,
        wftask_type_filters=wftask.type_filters,
    )

    wftask_db = await _workflow_insert_task(
        workflow_id=workflow.id,
        task_id=task_id,
        meta_non_parallel=wftask.meta_non_parallel,
        meta_parallel=wftask.meta_parallel,
        args_non_parallel=wftask.args_non_parallel,
        args_parallel=wftask.args_parallel,
        type_filters=wftask.type_filters,
        db=db,
    )

    return wftask_db


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
) -> WorkflowTaskReadV2 | None:
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
    if workflow_task_update.type_filters is not None:
        _check_type_filters_compatibility(
            task_input_types=db_wf_task.task.input_types,
            wftask_type_filters=workflow_task_update.type_filters,
        )

    if db_wf_task.task_type == TaskType.PARALLEL and (
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
    elif db_wf_task.task_type in [
        TaskType.NON_PARALLEL,
        TaskType.CONVERTER_NON_PARALLEL,
    ] and (
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

    for key, value in workflow_task_update.model_dump(
        exclude_unset=True
    ).items():
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
        elif key in ["meta_parallel", "meta_non_parallel", "type_filters"]:
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

    if await _workflow_has_submitted_job(workflow_id=workflow_id, db=db):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "Cannot delete a WorkflowTask while a Job is running for this "
                "Workflow."
            ),
        )

    # Delete WorkflowTask
    await db.delete(db_workflow_task)
    await db.commit()

    await db.refresh(db_workflow)
    db_workflow.task_list.reorder()
    await db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)
