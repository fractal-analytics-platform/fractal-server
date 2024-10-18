"""
`api` module
"""
import os

from fastapi import APIRouter
from fastapi import Depends

from ....config import get_settings
from ....syringe import Inject
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth import current_active_superuser


router_api = APIRouter()


@router_api.get("/alive/")
async def alive():
    pid = os.getpid()
    return {"pid": pid}


@router_api.get("/settings/")
async def view_settings(user: UserOAuth = Depends(current_active_superuser)):
    settings = Inject(get_settings)
    return settings.get_sanitized()
