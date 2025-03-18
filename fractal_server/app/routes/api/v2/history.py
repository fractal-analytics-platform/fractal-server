from datetime import datetime
from typing import Any
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi.responses import JSONResponse
from pydantic import AwareDatetime
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import field_serializer
from sqlmodel import func
from sqlmodel import select

from ._aux_functions import _get_workflow_check_owner
from ._aux_functions import _get_workflowtask_check_history_owner
from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import HistoryImageCache
from fractal_server.app.models.v2 import HistoryRun
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.routes.auth import current_active_user


router = APIRouter()


@router.get("/project/{project_id}/status/")
async def get_workflow_tasks_statuses(
    project_id: int,
    dataset_id: int,
    workflow_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> JSONResponse:
    workflow = await _get_workflow_check_owner(
        project_id=project_id,
        workflow_id=workflow_id,
        user_id=user.id,
        db=db,
    )
    response = {}
    for wftask in workflow.task_list:
        res = await db.execute(
            select(HistoryRun)
            .where(HistoryRun.dataset_id == dataset_id)
            .where(HistoryRun.workflowtask_id == wftask.id)
            .order_by(HistoryRun.timestamp_started.desc())
            .limit(1)
        )
        latest_history_run = res.scalar()
        if not latest_history_run:
            response[wftask.id] = None
            continue
        response[wftask.id] = dict(
            status=latest_history_run.status,
            num_available_images=latest_history_run.num_available_images,
        )

        for target_status in [
            "done",
            "submitted",
            "failed",
        ]:  # FIXME: use enum
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

    return JSONResponse(content=response, status_code=200)


class HistoryUnitRead(BaseModel):  # FIXME: Move to schemas

    id: int
    history_run_id: int
    logfile: Optional[str] = None
    status: str
    zarr_urls: list[str]


class HistoryRunRead(BaseModel):  # FIXME: move to schemas
    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: int
    dataset_id: int
    workflowtask_id: int
    workflowtask_dump: dict[str, Any]
    task_group_dump: dict[str, Any]
    timestamp_started: AwareDatetime
    status: str
    num_available_images: int
    units: list[HistoryUnit]

    @field_serializer("timestamp_started")
    def serialize_datetime(v: datetime) -> str:
        return v.isoformat()


@router.get("/project/{project_id}/status/run/")
async def get_history_runs(
    project_id: int,
    dataset_id: int,
    workflowtask_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[HistoryRunRead]:

    # Access control
    await _get_workflowtask_check_history_owner(
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

    for ind, run in enumerate(runs):
        stm = (
            select(HistoryUnit)
            .where(HistoryUnit.history_run_id == run.id)
            .order_by(HistoryUnit.id)
        )
        res = await db.execute(stm)
        units = res.scalars().all()
        runs[ind] = dict(
            **run.model_dump(), units=[unit.model_dump() for unit in units]
        )

    return runs


@router.get("/project/{project_id}/status/run/{history_run_id}/")
async def get_history_units(
    project_id: int,
    dataset_id: int,
    workflowtask_id: int,
    history_run_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[HistoryUnitRead]:

    # Access control
    await _get_workflowtask_check_history_owner(
        dataset_id=dataset_id,
        workflowtask_id=workflowtask_id,
        user_id=user.id,
        db=db,
    )

    res = await db.execute(
        select(HistoryRun)
        .where(HistoryRun.id == history_run_id)
        .where(HistoryRun.dataset_id == dataset_id)
        .where(HistoryRun.workflowtask_id == workflowtask_id)
    )
    history_run = res.scalar_one_or_none()
    if history_run is None:
        raise HTTPException(status_code=404, detail="HistoryRun not found.")

    # Get all units
    stm = (
        select(HistoryUnit)
        .where(HistoryUnit.history_run_id == history_run_id)
        .order_by(HistoryUnit.id)
    )
    res = await db.execute(stm)
    units = res.scalars().all()
    return units
