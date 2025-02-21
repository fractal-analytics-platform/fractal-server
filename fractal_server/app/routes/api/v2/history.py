from fastapi import APIRouter
from fastapi import Depends
from fastapi import status
from fastapi.responses import JSONResponse
from sqlmodel import desc
from sqlmodel import select

from ._aux_functions import _get_dataset_check_owner
from ._aux_functions import _get_workflow_check_history_owner
from ._aux_functions import _get_workflowtask_check_history_owner
from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.history.parse_history import get_workflow_statuses
from fractal_server.app.history.parse_history import (
    get_workflowtask_image_statuses,
)
from fractal_server.app.history.status_enum import HistoryItemImageStatus
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


@router.get("/history/details/")
async def get_history_details(
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

    stm = (
        select(HistoryItemV2)
        .where(HistoryItemV2.workflowtask_id == workflowtask_id)
        .where(HistoryItemV2.dataset_id == dataset_id)
        .order_by(desc(HistoryItemV2.timestamp_started))
    )
    res = await db.execute(stm)
    history = res.scalars().all()

    response = list()
    images_done = set()

    for hist_item in history:
        index = next(
            (
                index
                for index, res_item in enumerate(response)
                if res_item["hash"] == hist_item.parameters_hash
            ),
            None,
        )
        if index is None:
            response.append(
                {
                    "hash": hist_item.parameters_hash,
                    "wftask_dump": hist_item.workflowtask_dump,
                    "images": {
                        HistoryItemImageStatus.DONE: 0,
                        HistoryItemImageStatus.FAILED: 0,
                        HistoryItemImageStatus.SUBMITTED: 0,
                    },
                }
            )
            index = -1

        for image, image_status in hist_item.images.items():
            if image not in images_done:
                response[index]["images"][image_status] += 1
                images_done.add(image)

    response = [
        response_item
        for response_item in response[::-1]
        if response_item["images"]
        != {
            HistoryItemImageStatus.DONE: 0,
            HistoryItemImageStatus.FAILED: 0,
            HistoryItemImageStatus.SUBMITTED: 0,
        }
    ]

    return JSONResponse(content=response, status_code=200)
