from fastapi import APIRouter
from fastapi import Depends
from fastapi.responses import JSONResponse
from sqlmodel import func
from sqlmodel import select

from ._aux_functions import _get_workflow_check_owner
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
        last_history_run = res.scalar()
        if not last_history_run:
            response[wftask.id] = None
        else:
            res = await db.execute(
                select(func.count(HistoryImageCache))
                .join(HistoryUnit)
                .where(HistoryUnit.status == "done")
                .where(
                    HistoryImageCache.latest_history_unit_id == HistoryUnit.id
                )
                .where(HistoryImageCache.dataset_id == dataset_id)
                .where(HistoryImageCache.workflowtask_id == wftask.id)
            )
            num_done_images = res.all()

            res = await db.execute(
                select(func.count(HistoryImageCache))
                .join(HistoryUnit)
                .where(HistoryUnit.status == "submitted")
                .where(
                    HistoryImageCache.latest_history_unit_id == HistoryUnit.id
                )
                .where(HistoryImageCache.dataset_id == dataset_id)
                .where(HistoryImageCache.workflowtask_id == wftask.id)
            )
            num_submitted_images = res.all()

            res = await db.execute(
                select(func.count(HistoryImageCache))
                .join(HistoryUnit)
                .where(HistoryUnit.status == "failed")
                .where(
                    HistoryImageCache.latest_history_unit_id == HistoryUnit.id
                )
                .where(HistoryImageCache.dataset_id == dataset_id)
                .where(HistoryImageCache.workflowtask_id == wftask.id)
            )
            num_failed_images = res.all()

            response[wftask.id] = {
                "latest_status": last_history_run.status,
                "num_done_images": num_done_images,
                "num_submitted_images": num_submitted_images,
                "num_failed_images": num_failed_images,
            }

    return JSONResponse(content=response, status_code=200)
