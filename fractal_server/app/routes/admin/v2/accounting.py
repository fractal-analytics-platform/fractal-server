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
from fractal_server.app.routes.auth import current_active_superuser
from fractal_server.app.routes.pagination import get_pagination_params
from fractal_server.app.routes.pagination import PaginationRequest
from fractal_server.app.routes.pagination import PaginationResponse
from fractal_server.app.schemas.v2 import AccountingRecordRead


class AccountingQuery(BaseModel):
    user_id: int | None = None
    timestamp_min: AwareDatetime | None = None
    timestamp_max: AwareDatetime | None = None


router = APIRouter()


@router.post("/", response_model=PaginationResponse[AccountingRecordRead])
async def query_accounting(
    query: AccountingQuery,
    # Dependencies
    pagination: PaginationRequest = Depends(get_pagination_params),
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> PaginationResponse[AccountingRecordRead]:
    page = pagination.page
    page_size = pagination.page_size

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

    res_total_count = await db.execute(stm_count)
    total_count = res_total_count.scalar()

    if page_size is not None:
        stm = stm.offset((page - 1) * page_size).limit(page_size)
    else:
        page_size = total_count

    res = await db.execute(stm)
    records = res.scalars().all()

    return PaginationResponse[AccountingRecordRead](
        total_count=total_count,
        page_size=page_size,
        current_page=page,
        items=[record.model_dump() for record in records],
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
