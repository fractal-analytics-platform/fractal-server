from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Request
from fastapi import status
from fastapi.responses import JSONResponse

from ....db import AsyncSession
from ....db import get_async_db
from ....models import UserOAuth
from ...api.v2.healthckeck import HealthCheck
from ...api.v2.healthckeck import run_healthcheck
from ...auth import current_active_superuser

router = APIRouter()


@router.post("/", status_code=status.HTTP_200_OK)
async def run_admin_healthcheck(
    payload: HealthCheck,
    request: Request,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> JSONResponse:
    return run_healthcheck(payload=payload, request=request, user=user, db=db)


@router.post("/{user_id}/", status_code=status.HTTP_200_OK)
async def run_user_healthcheck(
    user_id: int,
    payload: HealthCheck,
    request: Request,
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> JSONResponse:
    user = await db.get(UserOAuth, user_id)
    if user is None:
        raise HTTPException(404, detail="User not found")
    return run_healthcheck(payload=payload, request=request, user=user, db=db)
