from copy import deepcopy
from typing import Any

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status
from fastapi.responses import JSONResponse
from sqlmodel import func
from sqlmodel import select

from ._aux_functions import _get_dataset_check_owner
from ._aux_functions import _get_workflow_check_owner
from ._aux_functions_history import _verify_workflow_and_dataset_access
from ._aux_functions_history import get_history_run_or_404
from ._aux_functions_history import get_history_unit_or_404
from ._aux_functions_history import get_wftask_check_owner
from ._aux_functions_history import read_log_file
from .images import ImageQuery
from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import HistoryImageCache
from fractal_server.app.models.v2 import HistoryRun
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.routes.auth import current_active_user
from fractal_server.app.routes.pagination import get_pagination_params
from fractal_server.app.routes.pagination import PaginationRequest
from fractal_server.app.routes.pagination import PaginationResponse
from fractal_server.app.schemas.v2 import HistoryRunRead
from fractal_server.app.schemas.v2 import HistoryRunReadAggregated
from fractal_server.app.schemas.v2 import HistoryUnitRead
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from fractal_server.app.schemas.v2 import HistoryUnitStatusQuery
from fractal_server.app.schemas.v2 import ImageLogsRequest
from fractal_server.app.schemas.v2 import SingleImageWithStatus
from fractal_server.images.tools import aggregate_attributes
from fractal_server.images.tools import aggregate_types
from fractal_server.images.tools import filter_image_list
from fractal_server.images.tools import merge_type_filters
from fractal_server.logger import set_logger


def check_historyrun_related_to_dataset_and_wftask(
    history_run: HistoryRun,
    dataset_id: int,
    workflowtask_id: int,
):
    if (
        history_run.dataset_id != dataset_id
        or history_run.workflowtask_id != workflowtask_id
    ):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Invalid query parameters: HistoryRun[{history_run.id}] is "
                f"not related to {dataset_id=} and {workflowtask_id=}."
            ),
        )


class ImageWithStatusPage(PaginationResponse[SingleImageWithStatus]):

    attributes: dict[str, list[Any]]
    types: list[str]


router = APIRouter()
logger = set_logger(__name__)


@router.get("/project/{project_id}/status/")
async def get_workflow_tasks_statuses(
    project_id: int,
    dataset_id: int,
    workflow_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> JSONResponse:

    # Access control
    workflow = await _get_workflow_check_owner(
        project_id=project_id,
        workflow_id=workflow_id,
        user_id=user.id,
        db=db,
    )
    await _get_dataset_check_owner(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,
        db=db,
    )

    response: dict[int, dict[str, int | str] | None] = {}
    for wftask in workflow.task_list:
        res = await db.execute(
            select(HistoryRun)
            .where(HistoryRun.dataset_id == dataset_id)
            .where(HistoryRun.workflowtask_id == wftask.id)
            .order_by(HistoryRun.timestamp_started.desc())
            .limit(1)
        )
        latest_history_run = res.scalar_one_or_none()
        if latest_history_run is None:
            logger.debug(
                f"No HistoryRun found for {dataset_id=} and {wftask.id=}."
            )
            response[wftask.id] = None
            continue
        response[wftask.id] = dict(
            status=latest_history_run.status,
            num_available_images=latest_history_run.num_available_images,
        )

        for target_status in HistoryUnitStatus:
            stm = (
                select(func.count(HistoryImageCache.zarr_url))
                .join(HistoryUnit)
                .where(HistoryImageCache.dataset_id == dataset_id)
                .where(HistoryImageCache.workflowtask_id == wftask.id)
                .where(
                    HistoryImageCache.latest_history_unit_id == HistoryUnit.id
                )
                .where(HistoryUnit.status == target_status)
            )
            res = await db.execute(stm)
            num_images = res.scalar()
            response[wftask.id][f"num_{target_status}_images"] = num_images

    new_response = deepcopy(response)
    for key, value in response.items():
        if value is not None:
            num_total_images = sum(
                value[f"num_{target_status}_images"]
                for target_status in HistoryUnitStatus
            )
            if num_total_images > value["num_available_images"]:
                value["num_available_images"] = None
        new_response[key] = value

    return JSONResponse(content=new_response, status_code=200)


@router.get("/project/{project_id}/status/run/")
async def get_history_run_list(
    project_id: int,
    dataset_id: int,
    workflowtask_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[HistoryRunReadAggregated]:

    # Access control
    await get_wftask_check_owner(
        project_id=project_id,
        dataset_id=dataset_id,
        workflowtask_id=workflowtask_id,
        user_id=user.id,
        db=db,
    )

    # Get all runs
    stm = (
        select(HistoryRun)
        .where(HistoryRun.dataset_id == dataset_id)
        .where(HistoryRun.workflowtask_id == workflowtask_id)
        .order_by(HistoryRun.timestamp_started)
    )
    res = await db.execute(stm)
    runs = res.scalars().all()

    # Respond early if there are no runs
    if not runs:
        return []

    # Add units count by status
    run_ids = [run.id for run in runs]
    stm = (
        select(
            HistoryUnit.history_run_id,
            HistoryUnit.status,
            func.count(HistoryUnit.id),
        )
        .where(HistoryUnit.history_run_id.in_(run_ids))
        .group_by(HistoryUnit.history_run_id, HistoryUnit.status)
    )
    res = await db.execute(stm)
    unit_counts = res.all()

    count_map = {
        run_id: {
            "num_done_units": 0,
            "num_submitted_units": 0,
            "num_failed_units": 0,
        }
        for run_id in run_ids
    }
    for run_id, unit_status, count in unit_counts:
        count_map[run_id][f"num_{unit_status}_units"] = count

    runs = [dict(**run.model_dump(), **count_map[run.id]) for run in runs]

    return runs


@router.get("/project/{project_id}/status/run/{history_run_id}/units/")
async def get_history_run_units(
    project_id: int,
    dataset_id: int,
    workflowtask_id: int,
    history_run_id: int,
    unit_status: HistoryUnitStatus | None = None,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
    pagination: PaginationRequest = Depends(get_pagination_params),
) -> PaginationResponse[HistoryUnitRead]:

    # Access control
    await get_wftask_check_owner(
        project_id=project_id,
        dataset_id=dataset_id,
        workflowtask_id=workflowtask_id,
        user_id=user.id,
        db=db,
    )

    # Check that `HistoryRun` exists
    history_run = await get_history_run_or_404(
        history_run_id=history_run_id, db=db
    )
    check_historyrun_related_to_dataset_and_wftask(
        history_run=history_run,
        dataset_id=dataset_id,
        workflowtask_id=workflowtask_id,
    )

    # Count `HistoryUnit`s
    stmt = select(func.count(HistoryUnit.id)).where(
        HistoryUnit.history_run_id == history_run_id
    )
    if unit_status:
        stmt = stmt.where(HistoryUnit.status == unit_status)
    res = await db.execute(stmt)
    total_count = res.scalar()
    page_size = pagination.page_size or total_count

    # Query `HistoryUnit`s
    stmt = (
        select(HistoryUnit)
        .where(HistoryUnit.history_run_id == history_run_id)
        .order_by(HistoryUnit.id)
    )
    if unit_status:
        stmt = stmt.where(HistoryUnit.status == unit_status)
    stmt = stmt.offset((pagination.page - 1) * page_size).limit(page_size)
    res = await db.execute(stmt)
    units = res.scalars().all()

    return dict(
        current_page=pagination.page,
        page_size=page_size,
        total_count=total_count,
        items=units,
    )


@router.post("/project/{project_id}/status/images/")
async def get_history_images(
    project_id: int,
    dataset_id: int,
    workflowtask_id: int,
    request_body: ImageQuery,
    unit_status: HistoryUnitStatusQuery | None = None,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
    pagination: PaginationRequest = Depends(get_pagination_params),
) -> ImageWithStatusPage:

    # Access control and object retrieval
    wftask = await get_wftask_check_owner(
        project_id=project_id,
        dataset_id=dataset_id,
        workflowtask_id=workflowtask_id,
        user_id=user.id,
        db=db,
    )
    res = await _verify_workflow_and_dataset_access(
        project_id=project_id,
        workflow_id=wftask.workflow_id,
        dataset_id=dataset_id,
        user_id=user.id,
        db=db,
    )
    dataset = res["dataset"]
    workflow = res["workflow"]

    # Setup prefix for logging
    prefix = f"[DS{dataset.id}-WFT{wftask.id}-images]"

    # (1) Get the type-filtered list of dataset images

    # (1A) Reconstruct dataset type filters by starting from {} and making
    # incremental updates with `output_types` of all previous tasks
    inferred_dataset_type_filters = {}
    for current_wftask in workflow.task_list[0 : wftask.order]:
        inferred_dataset_type_filters.update(current_wftask.task.output_types)
    logger.debug(f"{prefix} {inferred_dataset_type_filters=}")
    # (1B) Compute type filters for the current wftask
    type_filters_patch = merge_type_filters(
        task_input_types=wftask.task.input_types,
        wftask_type_filters=wftask.type_filters,
    )
    logger.debug(f"{prefix} {type_filters_patch=}")
    # (1C) Combine dataset type filters (lower priority) and current-wftask
    # filters (higher priority)
    actual_filters = inferred_dataset_type_filters
    actual_filters.update(type_filters_patch)
    logger.debug(f"{prefix} {actual_filters=}")
    # (1D) Get all matching images from the dataset
    pre_filtered_dataset_images = filter_image_list(
        images=dataset.images,
        type_filters=inferred_dataset_type_filters,
    )
    filtered_dataset_images = filter_image_list(
        pre_filtered_dataset_images,
        type_filters=request_body.type_filters,
        attribute_filters=request_body.attribute_filters,
    )
    logger.debug(f"{prefix} {len(dataset.images)=}")
    logger.debug(f"{prefix} {len(filtered_dataset_images)=}")
    # (1E) Extract the list of URLs for filtered images
    filtered_dataset_images_url = list(
        img["zarr_url"] for img in filtered_dataset_images
    )

    # (2) Get `(zarr_url, status)` pairs for all images that have already
    # been processed, and
    # (3) When relevant, find images that have not been processed
    base_stmt = (
        select(HistoryImageCache.zarr_url, HistoryUnit.status)
        .join(HistoryUnit)
        .where(HistoryImageCache.dataset_id == dataset_id)
        .where(HistoryImageCache.workflowtask_id == workflowtask_id)
        .where(HistoryImageCache.latest_history_unit_id == HistoryUnit.id)
        .where(HistoryImageCache.zarr_url.in_(filtered_dataset_images_url))
    )

    if unit_status in [HistoryUnitStatusQuery.UNSET, None]:
        stmt = base_stmt.order_by(HistoryImageCache.zarr_url)
        res = await db.execute(stmt)
        list_processed_url_status = res.all()
        list_processed_url = list(
            item[0] for item in list_processed_url_status
        )
        list_non_processed_url_status = list(
            (url, None)
            for url in filtered_dataset_images_url
            if url not in list_processed_url
        )
        if unit_status == HistoryUnitStatusQuery.UNSET:
            list_processed_url_status = []
    else:
        stmt = base_stmt.where(HistoryUnit.status == unit_status).order_by(
            HistoryImageCache.zarr_url
        )
        res = await db.execute(stmt)
        list_processed_url_status = res.all()
        list_non_processed_url_status = []

    logger.debug(f"{prefix} {len(list_processed_url_status)=}")
    logger.debug(f"{prefix} {len(list_non_processed_url_status)=}")

    # (3) Combine outputs from 1 and 2
    full_list_url_status = (
        list_processed_url_status + list_non_processed_url_status
    )
    logger.debug(f"{prefix} {len(full_list_url_status)=}")

    attributes = aggregate_attributes(pre_filtered_dataset_images)
    types = aggregate_types(pre_filtered_dataset_images)

    sorted_list_url_status = sorted(
        full_list_url_status,
        key=lambda url_status: url_status[0],
    )
    logger.debug(f"{prefix} {len(sorted_list_url_status)=}")

    # Final list of objects

    total_count = len(sorted_list_url_status)
    page_size = pagination.page_size or total_count

    paginated_list_url_status = sorted_list_url_status[
        (pagination.page - 1) * page_size : pagination.page * page_size
    ]

    # Aggregate information to create 'SingleImageWithStatus'
    items = [
        {
            **filtered_dataset_images[
                filtered_dataset_images_url.index(url_status[0])
            ],
            "status": url_status[1],
        }
        for url_status in paginated_list_url_status
    ]

    return dict(
        current_page=pagination.page,
        page_size=page_size,
        total_count=total_count,
        items=items,
        attributes=attributes,
        types=types,
    )


@router.post("/project/{project_id}/status/image-log/")
async def get_image_log(
    project_id: int,
    request_data: ImageLogsRequest,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> JSONResponse:
    # Access control
    wftask = await get_wftask_check_owner(
        project_id=project_id,
        dataset_id=request_data.dataset_id,
        workflowtask_id=request_data.workflowtask_id,
        user_id=user.id,
        db=db,
    )

    # Get HistoryImageCache
    history_image_cache = await db.get(
        HistoryImageCache,
        (
            request_data.zarr_url,
            request_data.dataset_id,
            request_data.workflowtask_id,
        ),
    )
    if history_image_cache is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="HistoryImageCache not found",
        )
    # Get history unit
    history_unit = await get_history_unit_or_404(
        history_unit_id=history_image_cache.latest_history_unit_id,
        db=db,
    )

    # Get log or placeholder text
    log = read_log_file(
        logfile=history_unit.logfile,
        wftask=wftask,
        dataset_id=request_data.dataset_id,
    )
    return JSONResponse(content=log)


@router.get("/project/{project_id}/status/unit-log/")
async def get_history_unit_log(
    project_id: int,
    history_run_id: int,
    history_unit_id: int,
    workflowtask_id: int,
    dataset_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> JSONResponse:
    # Access control
    wftask = await get_wftask_check_owner(
        project_id=project_id,
        dataset_id=dataset_id,
        workflowtask_id=workflowtask_id,
        user_id=user.id,
        db=db,
    )

    # Get history unit
    history_unit = await get_history_unit_or_404(
        history_unit_id=history_unit_id,
        db=db,
    )

    if history_unit.history_run_id != history_run_id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Invalid query parameters: HistoryUnit[{history_unit_id}] "
                f"is not related to HistoryRun[{history_run_id}]"
            ),
        )
    history_run = await get_history_run_or_404(
        history_run_id=history_run_id, db=db
    )
    check_historyrun_related_to_dataset_and_wftask(
        history_run=history_run,
        dataset_id=dataset_id,
        workflowtask_id=workflowtask_id,
    )

    # Get log or placeholder text
    log = read_log_file(
        logfile=history_unit.logfile,
        wftask=wftask,
        dataset_id=dataset_id,
    )
    return JSONResponse(content=log)


@router.get("/project/{project_id}/dataset/{dataset_id}/history/")
async def get_dataset_history(
    project_id: int,
    dataset_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[HistoryRunRead]:
    """
    Returns a list of all HistoryRuns associated to a given dataset, sorted by
    timestamp.
    """
    # Access control
    await _get_dataset_check_owner(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,
        db=db,
    )

    res = await db.execute(
        select(HistoryRun)
        .where(HistoryRun.dataset_id == dataset_id)
        .order_by(HistoryRun.timestamp_started)
    )
    history_run_list = res.scalars().all()

    return history_run_list
