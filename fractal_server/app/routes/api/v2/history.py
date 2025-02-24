from fastapi import APIRouter
from fastapi import Depends
from fastapi import status
from fastapi.responses import JSONResponse
from sqlmodel import case
from sqlmodel import func
from sqlmodel import select

from ._aux_functions import _get_dataset_check_owner
from ._aux_functions import _get_workflow_check_history_owner
from ._aux_functions import _get_workflow_check_owner
from ._aux_functions import _get_workflowtask_check_history_owner
from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.history.parse_history import get_workflow_statuses
from fractal_server.app.history.parse_history import (
    get_workflowtask_image_statuses,
)
from fractal_server.app.history.status_enum import HistoryItemImageStatus
from fractal_server.app.models import UserOAuth
from fractal_server.app.models import WorkflowTaskV2
from fractal_server.app.models.v2 import HistoryItemV2
from fractal_server.app.models.v2 import ImageStatus
from fractal_server.app.routes.auth import current_active_user
from fractal_server.app.schemas.v2.history import HistoryItemV2Read

router = APIRouter()


@router.get(
    "/project/{project_id}/dataset/{dataset_id}/history/",
    response_model=list[HistoryItemV2Read],
)
async def get_dataset_history(
    project_id: int,
    dataset_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[HistoryItemV2Read]:
    await _get_dataset_check_owner(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,
        db=db,
    )

    stm = (
        select(HistoryItemV2)
        .where(HistoryItemV2.dataset_id == dataset_id)
        .order_by(HistoryItemV2.timestamp_started)
    )
    res = await db.execute(stm)
    items = res.scalars().all()
    return items


@router.get("/project/{project_id}/status/")
async def get_per_workflow_aggregated_info(
    project_id: int,
    workflow_id: int,
    dataset_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
):
    workflow = await _get_workflow_check_owner(
        project_id=project_id,
        workflow_id=workflow_id,
        user_id=user.id,
        db=db,
    )

    res = await db.execute(
        select(
            WorkflowTaskV2.id,
            func.coalesce(
                func.count(
                    case(
                        (ImageStatus.status == HistoryItemImageStatus.DONE, 1)
                    )
                ),
                0,
            ),
            func.coalesce(
                func.count(
                    case(
                        (
                            ImageStatus.status
                            == HistoryItemImageStatus.FAILED,
                            1,
                        )
                    )
                ),
                0,
            ),
            func.coalesce(
                func.count(
                    case(
                        (
                            ImageStatus.status
                            == HistoryItemImageStatus.SUBMITTED,
                            1,
                        )
                    )
                ),
                0,
            ),
        )
        .select_from(WorkflowTaskV2)
        .outerjoin(
            ImageStatus,
            (ImageStatus.workflowtask_id == WorkflowTaskV2.id)
            & (ImageStatus.dataset_id == dataset_id),
        )
        .where(WorkflowTaskV2.id.in_([wft.id for wft in workflow.task_list]))
        .group_by(WorkflowTaskV2.id)
    )

    return {
        _id: {
            HistoryItemImageStatus.DONE: done,
            HistoryItemImageStatus.FAILED: failed,
            HistoryItemImageStatus.SUBMITTED: submitted,
        }
        for _id, done, failed, submitted in res.fetchall()
    }


# -------


@router.get("/history/latest-status/")
async def get_workflow_dataset_latest_status(
    workflow_id: int,
    dataset_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> JSONResponse:
    workflowtask_ids = await _get_workflow_check_history_owner(
        dataset_id=dataset_id,
        workflow_id=workflow_id,
        user_id=user.id,
        db=db,
    )

    statuses = await get_workflow_statuses(
        dataset_id=dataset_id,
        workflowtask_ids=workflowtask_ids,
        db=db,
    )
    return JSONResponse(content=statuses, status_code=status.HTTP_200_OK)


@router.get("/history/latest-status/images/")
async def get_workflowtask_detailed_status(
    workflowtask_id: int,
    dataset_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> JSONResponse:
    await _get_workflowtask_check_history_owner(
        dataset_id=dataset_id,
        workflowtask_id=workflowtask_id,
        user_id=user.id,
        db=db,
    )

    images = await get_workflowtask_image_statuses(
        dataset_id=dataset_id,
        workflowtask_id=workflowtask_id,
        db=db,
    )
    return JSONResponse(content=images, status_code=status.HTTP_200_OK)
