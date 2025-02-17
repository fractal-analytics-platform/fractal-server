from fastapi import APIRouter
from fastapi import Depends
from fastapi.responses import JSONResponse
from fastapi_users.authentication import JWTStrategy

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth import current_active_superuser
from fractal_server.app.routes.auth._aux_auth import _user_or_404
from fractal_server.config import get_settings
from fractal_server.syringe import Inject

router = APIRouter()


@router.get("/{user_id}/")
async def impersonate_user(
    user_id: int,
    superuser: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> JSONResponse:
    user = await _user_or_404(user_id, db)

    settings = Inject(get_settings)
    jwt_strategy = JWTStrategy(
        secret=settings.JWT_SECRET_KEY,  # type: ignore
        lifetime_seconds=7200,  # 2 hours
    )
    token = await jwt_strategy.write_token(user)

    return JSONResponse(
        content={"access_token": token, "token_type": "bearer"},
        status_code=200,
    )
