from itertools import chain
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from pydantic.types import AwareDatetime
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import AccountingRecord
from fractal_server.app.models.v2 import AccountingRecordSlurm
from fractal_server.app.routes.auth import current_active_superuser
from fractal_server.app.routes.aux import _raise_if_naive_datetime
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
    page: int = 1,
    page_size: Optional[int] = None,
    # dependencies
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> AccountingPage:

    _raise_if_naive_datetime(query.timestamp_min, query.timestamp_max)

    if page < 1:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid pagination parameter: page={page} < 1",
        )

    stm = select(AccountingRecord)
    if query.user_id is not None:
        stm = stm.where(AccountingRecord.user_id == query.user_id)
    if query.timestamp_min is not None:
        stm = stm.where(AccountingRecord.timestamp >= query.timestamp_min)
    if query.timestamp_max is not None:
        stm = stm.where(AccountingRecord.timestamp <= query.timestamp_max)

    if page_size is None:
        if page > 1:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    f"Invalid pagination parameters: {page=}, {page_size=}."
                ),
            )
    else:
        if page_size <= 0:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    f"Invalid pagination parameter: page_size={page_size} <= 0"
                ),
            )
        stm = stm.offset((page - 1) * page_size).limit(page_size)

    res = await db.execute(stm)
    accounting_list = res.scalars().all()

    actual_page_size = page_size or len(accounting_list)
    return AccountingPage(
        total_count=len(accounting_list),
        page_size=actual_page_size,
        current_page=page,
        records=[record.model_dump() for record in accounting_list],
    )


@router.post("/slurm/")
async def query_accounting_slurm(
    query: AccountingQuery,
    # dependencies
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> JSONResponse:

    _raise_if_naive_datetime(query.timestamp_min, query.timestamp_max)

    stm = select(AccountingRecordSlurm)

    if query.user_id is not None:
        stm = stm.where(AccountingRecordSlurm.user_id == query.user_id)
    if query.timestamp_min is not None:
        stm = stm.where(AccountingRecordSlurm.timestamp >= query.timestamp_min)
    if query.timestamp_max is not None:
        stm = stm.where(AccountingRecordSlurm.timestamp <= query.timestamp_max)

    res = await db.execute(stm)
    accounting_list = res.scalars().all()
    await db.close()

    ids = list(
        chain(accounting.slurm_job_ids for accounting in accounting_list)
    )
    return JSONResponse(content=ids, status_code=200)
