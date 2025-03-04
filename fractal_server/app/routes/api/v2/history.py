from pathlib import Path
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlmodel import func
from sqlmodel import select

from ._aux_functions import _get_dataset_check_owner
from ._aux_functions import _get_workflow_check_owner
from ._aux_functions import _get_workflowtask_check_history_owner
from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.history.status_enum import HistoryItemImageStatus
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import HistoryItemV2
from fractal_server.app.models.v2 import ImageStatus
from fractal_server.app.routes.auth import current_active_user
from fractal_server.app.routes.pagination import get_pagination_params
from fractal_server.app.routes.pagination import PaginationRequest
from fractal_server.app.routes.pagination import PaginationResponse
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
) -> JSONResponse:
    workflow = await _get_workflow_check_owner(
        project_id=project_id,
        workflow_id=workflow_id,
        user_id=user.id,
        db=db,
    )

    wft_ids = [wftask.id for wftask in workflow.task_list]

    # num_available_images
    stm = (
        select(
            HistoryItemV2.workflowtask_id, HistoryItemV2.num_available_images
        )
        .where(HistoryItemV2.dataset_id == dataset_id)
        .where(HistoryItemV2.workflowtask_id.in_(wft_ids))
        .order_by(
            HistoryItemV2.workflowtask_id,
            HistoryItemV2.timestamp_started.desc(),
        )
        # https://www.postgresql.org/docs/current/sql-select.html#SQL-DISTINCT
        .distinct(HistoryItemV2.workflowtask_id)
    )
    res = await db.execute(stm)
    num_available_images = {k: v for k, v in res.all()}

    count = {}
    for _status in HistoryItemImageStatus:
        stm = (
            select(ImageStatus.workflowtask_id, func.count())
            .where(ImageStatus.dataset_id == dataset_id)
            .where(ImageStatus.workflowtask_id.in_(wft_ids))
            .where(ImageStatus.status == _status)
            # https://docs.sqlalchemy.org/en/20/tutorial/data_select.html#tutorial-group-by-w-aggregates
            .group_by(ImageStatus.workflowtask_id)
        )
        res = await db.execute(stm)
        count[_status] = {k: v for k, v in res.all()}

    result = {
        str(_id): None
        if _id not in num_available_images
        else {
            "num_available_images": num_available_images[_id],
            "num_done_images": count["done"].get(_id, 0),
            "num_submitted_images": count["submitted"].get(_id, 0),
            "num_failed_images": count["failed"].get(_id, 0),
        }
        for _id in wft_ids
    }

    return JSONResponse(content=result, status_code=200)


@router.get("/project/{project_id}/status/subsets/")
async def get_per_workflowtask_subsets_aggregated_info(
    project_id: int,
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
        select(ImageStatus.parameters_hash, func.array_agg(ImageStatus.status))
        .where(ImageStatus.dataset_id == dataset_id)
        .where(ImageStatus.workflowtask_id == workflowtask_id)
        .group_by(ImageStatus.parameters_hash)
    )
    res = await db.execute(stm)
    hash_to_statuses = res.all()

    subsets = []
    for parameters_hash, statuses in hash_to_statuses:
        # Get the oldest HistoryItemV2 matching with `parameters_hash`
        stm = (
            select(HistoryItemV2)
            .where(HistoryItemV2.workflowtask_id == workflowtask_id)
            .where(HistoryItemV2.dataset_id == dataset_id)
            .where(HistoryItemV2.parameters_hash == parameters_hash)
            .order_by(HistoryItemV2.timestamp_started)
            .limit(1)
        )
        res = await db.execute(stm)
        oldest_history_item = res.scalar_one()

        subsets.append(
            {
                "_timestamp": oldest_history_item.timestamp_started,
                "workflowtask_dump": oldest_history_item.workflowtask_dump,
                "parameters_hash": parameters_hash,
                "info": {
                    "num_done_images": statuses.count(
                        HistoryItemImageStatus.DONE
                    ),
                    "num_failed_images": statuses.count(
                        HistoryItemImageStatus.FAILED
                    ),
                    "num_submitted_images": statuses.count(
                        HistoryItemImageStatus.SUBMITTED
                    ),
                },
            }
        )

    # Use `_timestamp` values for sorting, and then drop them from the response
    sorted_results = sorted(subsets, key=lambda obj: obj["_timestamp"])
    [item.pop("_timestamp") for item in sorted_results]

    return JSONResponse(content=sorted_results, status_code=200)


@router.get("/project/{project_id}/status/images/")
async def get_per_workflowtask_images(
    project_id: int,
    workflowtask_id: int,
    dataset_id: int,
    status: HistoryItemImageStatus,
    parameters_hash: Optional[str] = None,
    # Dependencies
    pagination: PaginationRequest = Depends(get_pagination_params),
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> PaginationResponse[str]:

    page = pagination.page
    page_size = pagination.page_size

    await _get_workflowtask_check_history_owner(
        dataset_id=dataset_id,
        workflowtask_id=workflowtask_id,
        user_id=user.id,
        db=db,
    )

    total_count_stm = (
        select(func.count(ImageStatus.zarr_url))
        .where(ImageStatus.dataset_id == dataset_id)
        .where(ImageStatus.workflowtask_id == workflowtask_id)
        .where(ImageStatus.status == status)
    )
    query = (
        select(ImageStatus.zarr_url)
        .where(ImageStatus.dataset_id == dataset_id)
        .where(ImageStatus.workflowtask_id == workflowtask_id)
        .where(ImageStatus.status == status)
    )

    if parameters_hash is not None:
        total_count_stm = total_count_stm.where(
            ImageStatus.parameters_hash == parameters_hash
        )
        query = query.where(ImageStatus.parameters_hash == parameters_hash)

    res_total_count = await db.execute(total_count_stm)
    total_count = res_total_count.scalar()

    if page_size is not None:
        query = query.limit(page_size)
    else:
        page_size = total_count

    if page > 1:
        query = query.offset((page - 1) * page_size)

    res = await db.execute(query)
    images = res.scalars().all()

    return PaginationResponse[str](
        total_count=total_count,
        page_size=page_size,
        current_page=page,
        items=images,
    )


class ImageLogsRequest(BaseModel):
    workflowtask_id: int
    dataset_id: int
    zarr_url: str


@router.post("/project/{project_id}/status/image-logs/")
async def get_image_logs(
    project_id: int,
    request_data: ImageLogsRequest,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> JSONResponse:

    wftask = await _get_workflowtask_check_history_owner(
        dataset_id=request_data.dataset_id,
        workflowtask_id=request_data.workflowtask_id,
        user_id=user.id,
        db=db,
    )

    image_status = await db.get(
        ImageStatus,
        (
            request_data.zarr_url,
            request_data.workflowtask_id,
            request_data.dataset_id,
        ),
    )
    if image_status is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="ImageStatus not found",
        )

    if image_status.logfile is None:
        return JSONResponse(
            content=(
                f"Logs for task '{wftask.task.name}' in dataset "
                f"{request_data.dataset_id} are not yet available."
            )
        )

    logfile = Path(image_status.logfile)
    if not logfile.exists():
        return JSONResponse(
            content=(
                f"Error while retrieving logs for task '{wftask.task.name}' "
                f"in dataset {request_data.dataset_id}: "
                f"file '{logfile}' is not available."
            )
        )

    with logfile.open("r") as f:
        file_contents = f.read()

    return JSONResponse(content=file_contents)
