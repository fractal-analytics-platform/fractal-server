from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status
from sqlalchemy import func
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import HistoryImageCache
from fractal_server.app.models import HistoryRun
from fractal_server.app.models import HistoryUnit
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import JobV2
from fractal_server.app.routes.auth import get_api_guest
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from fractal_server.app.schemas.v2.job import JobWithTaskStatuses
from fractal_server.app.schemas.v2.job import TaskStatusImages
from fractal_server.app.schemas.v2.job import TaskStatusSimple
from fractal_server.app.schemas.v2.sharing import ProjectPermissions
from fractal_server.logger import set_logger

from ._aux_functions import _get_dataset_check_access
from ._aux_functions import _get_submitted_jobs_statement
from ._aux_functions import _get_workflow_check_access

router = APIRouter()
logger = set_logger(__name__)


@router.get(
    "/project/{project_id}/latest-job/",
    response_model=JobWithTaskStatuses,
)
async def get_latest_job(
    project_id: int,
    workflow_id: int,
    dataset_id: int,
    user: UserOAuth = Depends(get_api_guest),
    db: AsyncSession = Depends(get_async_db),
) -> JobV2:
    workflow = await _get_workflow_check_access(
        project_id=project_id,
        workflow_id=workflow_id,
        user_id=user.id,
        required_permissions=ProjectPermissions.READ,
        db=db,
    )
    await _get_dataset_check_access(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,
        required_permissions=ProjectPermissions.READ,
        db=db,
    )

    stm = (
        select(JobV2)
        .where(JobV2.project_id == project_id)
        .where(JobV2.workflow_id == workflow_id)
        .where(JobV2.dataset_id == dataset_id)
        .order_by(JobV2.start_timestamp.desc())
        .limit(1)
    )
    res = await db.execute(stm)
    latest_job = res.scalar_one_or_none()
    if latest_job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job with {workflow_id=} and {dataset_id=} not found.",
        )

    res = await db.execute(
        _get_submitted_jobs_statement()
        .where(JobV2.dataset_id == dataset_id)
        .where(JobV2.workflow_id == workflow_id)
    )
    running_job = res.scalars().one_or_none()

    if running_job is not None:
        running_wftasks = workflow.task_list[
            running_job.first_task_index : running_job.last_task_index + 1
        ]
        running_wftask_ids = [wft.id for wft in running_wftasks]
    else:
        running_wftask_ids = []

    statuses: dict[int, TaskStatusSimple | TaskStatusImages | None] = {}
    ids_to_skip = []

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
                statuses[wftask.id] = TaskStatusSimple(
                    status=HistoryUnitStatus.SUBMITTED
                )
            else:
                logger.debug(f"A2: No HistoryRun for {wftask.id=}.")
                statuses[wftask.id] = None
            ids_to_skip.append(wftask.id)
            continue
        else:
            if wftask.id in running_wftask_ids:
                if latest_run.job_id == running_job.id:
                    logger.debug(
                        f"B1 for {wftask.id} and {latest_run.job_id=}."
                    )
                    statuses[wftask.id] = TaskStatusImages(
                        status=latest_run.status
                    )
                else:
                    logger.debug(
                        f"B2 for {wftask.id} and {latest_run.job_id=}."
                    )
                    statuses[wftask.id] = TaskStatusImages(
                        status=HistoryUnitStatus.SUBMITTED
                    )
            else:
                logger.debug(f"C1: {wftask.id=} not in {running_wftask_ids=}.")
                statuses[wftask.id] = TaskStatusImages(status=latest_run.status)

        statuses[
            wftask.id
        ].num_available_images = latest_run.num_available_images

        stm = (
            select(HistoryUnit.status, func.count(HistoryImageCache.zarr_url))
            .join(
                HistoryUnit,
                HistoryImageCache.latest_history_unit_id == HistoryUnit.id,
            )
            .where(HistoryImageCache.dataset_id == dataset_id)
            .where(HistoryImageCache.workflowtask_id == wftask.id)
            .group_by(HistoryUnit.status)
        )

        res = await db.execute(stm)
        rows = res.all()  # list of (status, num_images)

        # initialize zeros for all statuses
        for target_status in HistoryUnitStatus:
            setattr(statuses[wftask.id], f"num_{target_status}_images", 0)

        for target_status, num_images in rows:
            setattr(
                statuses[wftask.id], f"num_{target_status}_images", num_images
            )

    # Set `num_available_images=None` for cases where it would be
    # smaller than `num_total_images`
    statuses_update = {}
    for wftask_id, status_value in statuses.items():
        if wftask_id in ids_to_skip:
            # Skip cases where status has no image counters
            continue
        num_total_images = sum(
            getattr(status_value, f"num_{target_status}_images")
            for target_status in HistoryUnitStatus
        )
        if num_total_images > status_value.num_available_images:
            status_value.num_available_images = None
            statuses_update[wftask_id] = status_value
    statuses.update(statuses_update)

    return JobWithTaskStatuses(
        **latest_job.model_dump(), task_statuses=statuses
    )
