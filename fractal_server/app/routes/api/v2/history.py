from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Query
from fastapi import status
from fastapi.responses import JSONResponse
from sqlmodel import func
from sqlmodel import select

from ._aux_functions import _get_dataset_check_owner
from ._aux_functions import _get_workflow_check_history_owner
from ._aux_functions import _get_workflow_check_owner
from ._aux_functions import _get_workflow_task_check_owner
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
from fractal_server.app.models.v2 import ImageStatus
from fractal_server.app.models.v2 import WorkflowTaskV2
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


# ---


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

    result = {}
    for wftask in workflow.task_list:

        stm = (
            select(HistoryItemV2.num_available_images)
            .where(HistoryItemV2.dataset_id == dataset_id)
            .where(HistoryItemV2.workflowtask_id == wftask.id)
            .order_by(HistoryItemV2.timestamp_started.desc())
            .limit(1)
        )
        res = await db.execute(stm)
        num_available_images = res.scalar_one_or_none()

        if num_available_images is None:
            result[str(wftask.id)] = None
            continue

        done_stm = (
            select(func.count(ImageStatus.zarr_url))
            .where(ImageStatus.workflowtask_id == wftask.id)
            .where(ImageStatus.dataset_id == dataset_id)
            .where(ImageStatus.status == HistoryItemImageStatus.DONE)
        )
        failed_stm = (
            select(func.count(ImageStatus.zarr_url))
            .where(ImageStatus.workflowtask_id == wftask.id)
            .where(ImageStatus.dataset_id == dataset_id)
            .where(ImageStatus.status == HistoryItemImageStatus.FAILED)
        )
        submitted_stm = (
            select(func.count(ImageStatus.zarr_url))
            .where(ImageStatus.workflowtask_id == wftask.id)
            .where(ImageStatus.dataset_id == dataset_id)
            .where(ImageStatus.status == HistoryItemImageStatus.SUBMITTED)
        )

        done_res = await db.execute(done_stm)
        done = done_res.scalar()
        failed_res = await db.execute(failed_stm)
        failed = failed_res.scalar()
        submitted_res = await db.execute(submitted_stm)
        submitted = submitted_res.scalar()

        result[str(wftask.id)] = {
            "num_done_images": done,
            "num_failed_images": failed,
            "num_submitted_images": submitted,
            "num_available_images": num_available_images,
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
    wftask = await db.get(WorkflowTaskV2, workflowtask_id)
    if wftask is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="WorkflowTask not found",
        )
    await _get_workflow_task_check_owner(
        project_id=project_id,
        workflow_id=wftask.workflow_id,
        workflow_task_id=workflowtask_id,
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
    hash_statuses = res.all()

    result = []
    for _hash, statuses in hash_statuses:
        dump = await db.execute(
            select(HistoryItemV2.workflowtask_dump)
            .where(HistoryItemV2.workflowtask_id == workflowtask_id)
            .where(HistoryItemV2.dataset_id == dataset_id)
            .where(HistoryItemV2.parameters_hash == _hash)
        )
        result.append(
            {
                "workflowtask_dump": dump.scalar_one(),
                "parameters_hash": _hash,
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

    return JSONResponse(content=result, status_code=200)


@router.get("/project/{project_id}/status/images/")
async def get_per_workflowtask_images(
    project_id: int,
    workflowtask_id: int,
    dataset_id: int,
    status: HistoryItemImageStatus,
    parameters_hash: Optional[str] = None,
    # Pagination
    page: int = Query(default=1, ge=1),
    page_size: Optional[int] = Query(default=None, ge=1),
    # Dependencies
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> JSONResponse:

    if page_size is None and page > 1:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(f"Invalid pagination parameters: {page=}, {page_size=}."),
        )

    wftask = await db.get(WorkflowTaskV2, workflowtask_id)
    if wftask is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="WorkflowTask not found",
        )
    await _get_workflow_task_check_owner(
        project_id=project_id,
        workflow_id=wftask.workflow_id,
        workflow_task_id=workflowtask_id,
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

    if page_size is not None:
        query = query.limit(page_size)
    if page > 1:
        query = query.offset((page - 1) * page_size)

    res_total_count = await db.execute(total_count_stm)
    total_count = res_total_count.scalar()

    res = await db.execute(query)
    images = res.scalars().all()

    return {
        "total_count": total_count,
        "page_size": page_size,
        "current_page": page,
        "images": images,
    }
