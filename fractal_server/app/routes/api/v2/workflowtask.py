from copy import deepcopy
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from sqlmodel import delete
from sqlmodel import select

from ....db import AsyncSession
from ....db import get_async_db
from ._aux_functions import _get_workflow_check_owner
from ._aux_functions import _get_workflow_task_check_owner
from ._aux_functions import _workflow_insert_task
from ._aux_functions_tasks import _check_type_filters_compatibility
from ._aux_functions_tasks import _get_task_read_access
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import HistoryItemV2
from fractal_server.app.models.v2 import ImageStatus
from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.app.routes.auth import current_active_user
from fractal_server.app.schemas.v2 import WorkflowTaskCreateV2
from fractal_server.app.schemas.v2 import WorkflowTaskReadV2
from fractal_server.app.schemas.v2 import WorkflowTaskReplaceV2
from fractal_server.app.schemas.v2 import WorkflowTaskUpdateV2

router = APIRouter()


@router.post(
    "/project/{project_id}/workflow/{workflow_id}/wftask/replace-task/",
    response_model=WorkflowTaskReadV2,
    status_code=status.HTTP_201_CREATED,
)
async def replace_workflowtask(
    project_id: int,
    workflow_id: int,
    workflow_task_id: int,
    task_id: int,
    replace: Optional[WorkflowTaskReplaceV2] = None,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> WorkflowTaskReadV2:

    old_workflow_task, workflow = await _get_workflow_task_check_owner(
        project_id=project_id,
        workflow_id=workflow_id,
        workflow_task_id=workflow_task_id,
        user_id=user.id,
        db=db,
    )

    new_task = await _get_task_read_access(
        task_id=task_id, user_id=user.id, db=db, require_active=True
    )

    if new_task.type != old_workflow_task.task.type:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Cannot replace a Task '{old_workflow_task.task.type}' with a"
                f"  Task '{new_task.type}'."
            ),
        )

    _check_type_filters_compatibility(
        task_input_types=new_task.input_types,
        wftask_type_filters=old_workflow_task.type_filters,
    )

    _args_non_parallel = old_workflow_task.args_non_parallel
    _args_parallel = old_workflow_task.args_parallel
    if replace is not None:
        if replace.args_non_parallel is not None:
            if new_task.type == "parallel":
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=(
                        "Cannot set 'args_non_parallel' "
                        "when Task is 'parallel'."
                    ),
                )
            else:
                _args_non_parallel = replace.args_non_parallel

        if replace.args_parallel is not None:
            if new_task.type == "non_parallel":
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=(
                        "Cannot set 'args_parallel' "
                        "when Task is 'non_parallel'."
                    ),
                )
            else:
                _args_parallel = replace.args_parallel

    # If user's changes to `meta_non_parallel` are compatible with new task,
    # keep them; else, get `meta_non_parallel` from new task
    if (
        old_workflow_task.meta_non_parallel
        != old_workflow_task.task.meta_non_parallel
    ) and (
        old_workflow_task.task.meta_non_parallel == new_task.meta_non_parallel
    ):
        _meta_non_parallel = old_workflow_task.meta_non_parallel
    else:
        _meta_non_parallel = new_task.meta_non_parallel
    # Same for `meta_parallel`
    if (
        old_workflow_task.meta_parallel != old_workflow_task.task.meta_parallel
    ) and (old_workflow_task.task.meta_parallel == new_task.meta_parallel):
        _meta_parallel = old_workflow_task.meta_parallel
    else:
        _meta_parallel = new_task.meta_parallel

    new_workflow_task = WorkflowTaskV2(
        task_id=new_task.id,
        task_type=new_task.type,
        task=new_task,
        # old-task values
        type_filters=old_workflow_task.type_filters,
        # possibly new values
        args_non_parallel=_args_non_parallel,
        args_parallel=_args_parallel,
        meta_non_parallel=_meta_non_parallel,
        meta_parallel=_meta_parallel,
    )

    workflow_task_order = old_workflow_task.order
    workflow.task_list.remove(old_workflow_task)
    workflow.task_list.insert(workflow_task_order, new_workflow_task)
    await db.commit()
    await db.refresh(new_workflow_task)
    return new_workflow_task


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
    elif task.type == "non_parallel":
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

    await db.close()

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
    if workflow_task_update.type_filters is not None:
        _check_type_filters_compatibility(
            task_input_types=db_wf_task.task.input_types,
            wftask_type_filters=workflow_task_update.type_filters,
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

    # Cascade operations:
    # * set foreign-keys to null for history items which are in relationship
    #   with the current workflowtask;
    # * delete ImageStatus in relationship with the current workflowtask.
    stm = select(HistoryItemV2).where(
        HistoryItemV2.workflowtask_id == db_workflow_task.id
    )
    res = await db.execute(stm)
    history_items = res.scalars().all()
    for history_item in history_items:
        history_item.workflowtask_id = None

    stm = delete(ImageStatus).where(
        ImageStatus.workflowtask_id == db_workflow_task.id
    )
    await db.execute(stm)

    # Delete WorkflowTask
    await db.delete(db_workflow_task)
    await db.commit()

    await db.refresh(db_workflow)
    db_workflow.task_list.reorder()
    await db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)
