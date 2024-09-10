"""
`api` module
"""
from fastapi import APIRouter
from fastapi import Depends

from ....config import get_settings
from ....syringe import Inject
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth import current_active_superuser


router_api = APIRouter()


@router_api.get("/alive/")
async def alive():
    settings = Inject(get_settings)
    return dict(
        alive=True,
        version=settings.PROJECT_VERSION,
    )


@router_api.get("/settings/")
async def view_settings(user: UserOAuth = Depends(current_active_superuser)):
    settings = Inject(get_settings)
    return settings.get_sanitized()
