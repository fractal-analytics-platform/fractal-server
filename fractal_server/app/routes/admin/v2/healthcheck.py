from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Request
from fastapi import status

from ....db import AsyncSession
from ....db import get_async_db
from ....models import UserOAuth
from ....schemas.v2 import JobReadV2
from ...api.v2.healthckeck import HealthCheck
from ...api.v2.healthckeck import run_healthcheck
from ...auth import current_active_superuser

router = APIRouter()


@router.post(
    "/{user_id}/", status_code=status.HTTP_200_OK, response_model=JobReadV2
)
async def run_user_healthcheck(
    user_id: int,
    payload: HealthCheck,
    request: Request,
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> JobReadV2:
    user = await db.get(UserOAuth, user_id)
    if user is None:
        raise HTTPException(404, detail="User not found")
    return await run_healthcheck(
        payload=payload, request=request, user=user, db=db
    )
