from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status
from fastapi.responses import JSONResponse
from sqlmodel import func
from sqlmodel import select

from ._aux_functions import _get_dataset_check_owner
from ._aux_functions import _get_submitted_job_or_none
from ._aux_functions import _get_workflow_check_owner
from ._aux_functions_history import _verify_workflow_and_dataset_access
from ._aux_functions_history import get_history_run_or_404
from ._aux_functions_history import get_history_unit_or_404
from ._aux_functions_history import get_wftask_check_owner
from ._aux_functions_history import read_log_file
from .images import ImagePage
from .images import ImageQuery
from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import HistoryImageCache
from fractal_server.app.models.v2 import HistoryRun
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.routes.auth import current_active_user
from fractal_server.app.routes.pagination import get_pagination_params
from fractal_server.app.routes.pagination import PaginationRequest
from fractal_server.app.routes.pagination import PaginationResponse
from fractal_server.app.schemas.v2 import HistoryRunRead
from fractal_server.app.schemas.v2 import HistoryRunReadAggregated
from fractal_server.app.schemas.v2 import HistoryUnitRead
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from fractal_server.app.schemas.v2 import HistoryUnitStatusWithUnset
from fractal_server.app.schemas.v2 import ImageLogsRequest
from fractal_server.images.status_tools import enrich_images_unsorted_async
from fractal_server.images.status_tools import IMAGE_STATUS_KEY
from fractal_server.images.tools import aggregate_attributes
from fractal_server.images.tools import aggregate_types
from fractal_server.images.tools import filter_image_list
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

    running_job = await _get_submitted_job_or_none(
        db=db,
        dataset_id=dataset_id,
        workflow_id=workflow_id,
    )
    if running_job is not None:
        running_wftasks = workflow.task_list[
            running_job.first_task_index : running_job.last_task_index + 1
        ]
        running_wftask_ids = [wft.id for wft in running_wftasks]
    else:
        running_wftask_ids = []

    response: dict[int, dict[str, int | str] | None] = {}
    for wftask in workflow.task_list:
        res = await db.execute(
            select(HistoryRun)
            .where(HistoryRun.dataset_id == dataset_id)
            .where(HistoryRun.workflowtask_id == wftask.id)
            .order_by(HistoryRun.timestamp_started.desc())
            .limit(1)
        )
        latest_run = res.scalar_one_or_none()

        if latest_run is None:
            if wftask.id in running_wftask_ids:
                logger.debug(f"A1: No HistoryRun for {wftask.id=}.")
                response[wftask.id] = dict(status=HistoryUnitStatus.SUBMITTED)
            else:
                logger.debug(f"A2: No HistoryRun for {wftask.id=}.")
                response[wftask.id] = None
            continue
        else:
            if wftask.id in running_wftask_ids:
                if latest_run.job_id == running_job.id:
                    logger.debug(
                        f"B1 for {wftask.id} and {latest_run.job_id=}."
                    )
                    response[wftask.id] = dict(status=latest_run.status)
                else:
                    logger.debug(
                        f"B2 for {wftask.id} and {latest_run.job_id=}."
                    )
                    response[wftask.id] = dict(
                        status=HistoryUnitStatus.SUBMITTED
                    )
            else:
                logger.debug(f"C1: {wftask.id=} not in {running_wftask_ids=}.")
                response[wftask.id] = dict(status=latest_run.status)

        response[wftask.id][
            "num_available_images"
        ] = latest_run.num_available_images

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

    # Set `num_available_images=None` for cases where it would be
    # smaller than `num_total_images`
    values_to_skip = (None, {"status": HistoryUnitStatus.SUBMITTED})
    response_update = {}
    for wftask_id, status_value in response.items():
        if status_value in values_to_skip:
            # Skip cases where status has no image counters
            continue
        num_total_images = sum(
            status_value[f"num_{target_status}_images"]
            for target_status in HistoryUnitStatus
        )
        if num_total_images > status_value["num_available_images"]:
            status_value["num_available_images"] = None
            response_update[wftask_id] = status_value
    response.update(response_update)

    return JSONResponse(content=response, status_code=200)


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

    res = await db.execute(
        select(
            TaskV2.id,
            TaskV2.version,
            TaskV2.args_schema_parallel,
            TaskV2.args_schema_non_parallel,
        ).where(
            TaskV2.id.in_(
                [run.task_id for run in runs if run.task_id is not None]
            )
        )
    )

    task_args = {
        _id: {
            "version": version,
            "args_schema_parallel": parallel,
            "args_schema_non_parallel": non_parallel,
        }
        for _id, version, parallel, non_parallel in res.all()
    }

    runs = [
        dict(
            **run.model_dump(),
            **count_map[run.id],
            **task_args.get(run.task_id, {}),
        )
        for run in runs
    ]

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
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
    pagination: PaginationRequest = Depends(get_pagination_params),
) -> ImagePage:
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

    # Setup prefix for logging
    prefix = f"[DS{dataset.id}-WFT{workflowtask_id}-images]"

    # (1) Apply type filters
    type_filtered_images = filter_image_list(
        images=dataset.images,
        type_filters=request_body.type_filters,
    )

    # (2) Extract valid values for attributes and types
    attributes = aggregate_attributes(type_filtered_images)
    attributes[IMAGE_STATUS_KEY] = [
        HistoryUnitStatusWithUnset.DONE,
        HistoryUnitStatusWithUnset.SUBMITTED,
        HistoryUnitStatusWithUnset.FAILED,
        HistoryUnitStatusWithUnset.UNSET,
    ]
    types = aggregate_types(type_filtered_images)

    # (3) Enrich images with status attribute
    type_filtered_images_with_status = await enrich_images_unsorted_async(
        dataset_id=dataset_id,
        workflowtask_id=workflowtask_id,
        images=type_filtered_images,
        db=db,
    )

    # (4) Apply attribute filters
    final_images_with_status = filter_image_list(
        type_filtered_images_with_status,
        attribute_filters=request_body.attribute_filters,
    )

    logger.debug(f"{prefix} {len(dataset.images)=}")
    logger.debug(f"{prefix} {len(final_images_with_status)=}")

    # (5) Apply pagination logic
    total_count = len(final_images_with_status)
    page_size = pagination.page_size or total_count
    sorted_images_list = sorted(
        final_images_with_status,
        key=lambda image: image["zarr_url"],
    )
    paginated_images_list = sorted_images_list[
        (pagination.page - 1) * page_size : pagination.page * page_size
    ]

    return dict(
        current_page=pagination.page,
        page_size=page_size,
        total_count=total_count,
        items=paginated_images_list,
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
