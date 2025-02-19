from fastapi import APIRouter
from fastapi import Depends
from fastapi import status
from fastapi.responses import JSONResponse
from sqlmodel import select

from ._aux_functions import _get_dataset_check_owner
from ._aux_functions import _history_access_control
from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.history.parse_history import (
    parse_history_given_async_db,
)
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import HistoryItemV2
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


@router.get(
    "/history/",
    response_model=list[HistoryItemV2Read],
)
async def get_wftask_dataset_history(
    dataset_id: int,
    workflowtask_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[HistoryItemV2Read]:
    await _history_access_control(
        dataset_id=dataset_id,
        workflow_task_id=workflowtask_id,
        user_id=user.id,
        db=db,
    )

    stm = (
        select(HistoryItemV2)
        .where(HistoryItemV2.dataset_id == dataset_id)
        .where(HistoryItemV2.workflowtask_id == workflowtask_id)
        .order_by(HistoryItemV2.timestamp_started)
    )
    res = await db.execute(stm)
    items = res.scalars().all()

    return items


@router.get(
    "/history/latest-status/",
)
async def get_wftask_dataset_latest_status(
    dataset_id: int,
    workflowtask_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> JSONResponse:
    await _history_access_control(
        dataset_id=dataset_id,
        workflow_task_id=workflowtask_id,
        user_id=user.id,
        db=db,
    )

    images = await parse_history_given_async_db(
        dataset_id=dataset_id,
        workflowtask_id=workflowtask_id,
        db=db,
    )
    return JSONResponse(
        content=images,
        status_code=status.HTTP_200_OK,
    )
