from itertools import chain

from fastapi import APIRouter
from fastapi import Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from pydantic.types import AwareDatetime
from sqlmodel import func
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import AccountingRecord
from fractal_server.app.models.v2 import AccountingRecordSlurm
from fractal_server.app.routes.auth import current_superuser_act
from fractal_server.app.routes.pagination import PaginationRequest
from fractal_server.app.routes.pagination import PaginationResponse
from fractal_server.app.routes.pagination import get_paginated_response
from fractal_server.app.routes.pagination import get_pagination_params
from fractal_server.app.schemas.v2 import AccountingRecordRead


class AccountingQuery(BaseModel):
    user_id: int | None = None
    timestamp_min: AwareDatetime | None = None
    timestamp_max: AwareDatetime | None = None
    fractal_job_id: int | None = None


router = APIRouter()


@router.post("/", response_model=PaginationResponse[AccountingRecordRead])
async def query_accounting(
    query: AccountingQuery,
    # Dependencies
    pagination: PaginationRequest = Depends(get_pagination_params),
    superuser: UserOAuth = Depends(current_superuser_act),
    db: AsyncSession = Depends(get_async_db),
) -> PaginationResponse[AccountingRecord]:
    stm = select(AccountingRecord).order_by(AccountingRecord.id)
    stm_count = select(func.count(AccountingRecord.id))
    if query.user_id is not None:
        stm = stm.where(AccountingRecord.user_id == query.user_id)
        stm_count = stm_count.where(AccountingRecord.user_id == query.user_id)
    if query.timestamp_min is not None:
        stm = stm.where(AccountingRecord.timestamp >= query.timestamp_min)
        stm_count = stm_count.where(
            AccountingRecord.timestamp >= query.timestamp_min
        )
    if query.timestamp_max is not None:
        stm = stm.where(AccountingRecord.timestamp <= query.timestamp_max)
        stm_count = stm_count.where(
            AccountingRecord.timestamp <= query.timestamp_max
        )

    paginated_response = await get_paginated_response(
        stm=stm,
        stm_count=stm_count,
        pagination=pagination,
        db=db,
    )
    return paginated_response


@router.post("/slurm/")
async def query_accounting_slurm(
    query: AccountingQuery,
    # dependencies
    superuser: UserOAuth = Depends(current_superuser_act),
    db: AsyncSession = Depends(get_async_db),
) -> JSONResponse:
    stm = select(AccountingRecordSlurm.slurm_job_ids)
    if query.user_id is not None:
        stm = stm.where(AccountingRecordSlurm.user_id == query.user_id)
    if query.timestamp_min is not None:
        stm = stm.where(AccountingRecordSlurm.timestamp >= query.timestamp_min)
    if query.timestamp_max is not None:
        stm = stm.where(AccountingRecordSlurm.timestamp <= query.timestamp_max)
    if query.fractal_job_id is not None:
        stm = stm.where(
            AccountingRecordSlurm.fractal_job_id == query.fractal_job_id
        )

    res = await db.execute(stm)
    nested_slurm_job_ids = res.scalars().all()
    aggregated_slurm_job_ids = list(chain(*nested_slurm_job_ids))
    return JSONResponse(content=aggregated_slurm_job_ids, status_code=200)
