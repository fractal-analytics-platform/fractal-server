from itertools import chain
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Query
from fastapi import status
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
from fractal_server.app.routes.auth import current_active_superuser
from fractal_server.app.schemas.v2 import AccountingRecordRead


class AccountingQuery(BaseModel):
    user_id: Optional[int] = None
    timestamp_min: Optional[AwareDatetime] = None
    timestamp_max: Optional[AwareDatetime] = None


class AccountingPage(BaseModel):
    total_count: int
    page_size: int
    current_page: int
    records: list[AccountingRecordRead]


router = APIRouter()


@router.post("/", response_model=AccountingPage)
async def query_accounting(
    query: AccountingQuery,
    # pagination
    page: int = Query(default=1, ge=1),
    page_size: Optional[int] = Query(default=None, ge=1),
    # dependencies
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> AccountingPage:

    if page_size is None and page > 1:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(f"Invalid pagination parameters: {page=}, {page_size=}."),
        )

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
    if page_size is not None:
        stm = stm.offset((page - 1) * page_size).limit(page_size)

    res = await db.execute(stm)
    records = res.scalars().all()
    res_total_count = await db.execute(stm_count)
    total_count = res_total_count.scalar()

    actual_page_size = page_size or len(records)
    return AccountingPage(
        total_count=total_count,
        page_size=actual_page_size,
        current_page=page,
        records=[record.model_dump() for record in records],
    )


@router.post("/slurm/")
async def query_accounting_slurm(
    query: AccountingQuery,
    # dependencies
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> JSONResponse:

    stm = select(AccountingRecordSlurm.slurm_job_ids)
    if query.user_id is not None:
        stm = stm.where(AccountingRecordSlurm.user_id == query.user_id)
    if query.timestamp_min is not None:
        stm = stm.where(AccountingRecordSlurm.timestamp >= query.timestamp_min)
    if query.timestamp_max is not None:
        stm = stm.where(AccountingRecordSlurm.timestamp <= query.timestamp_max)

    res = await db.execute(stm)
    nested_slurm_job_ids = res.scalars().all()
    aggregated_slurm_job_ids = list(chain(*nested_slurm_job_ids))
    return JSONResponse(content=aggregated_slurm_job_ids, status_code=200)
