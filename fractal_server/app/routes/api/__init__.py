"""
`api` module
"""
from fastapi import APIRouter
from fastapi import Depends

import fractal_server
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth import current_superuser_act
from fractal_server.config import get_data_settings
from fractal_server.config import get_db_settings
from fractal_server.config import get_email_settings
from fractal_server.config import get_oauth_settings
from fractal_server.config import get_settings
from fractal_server.syringe import Inject

router_api = APIRouter()


@router_api.get("/alive/")
async def alive():
    return dict(
        alive=True,
        version=fractal_server.__VERSION__,
    )


@router_api.get("/settings/app/")
async def view_settings(
    user: UserOAuth = Depends(current_superuser_act),
):
    settings = Inject(get_settings)
    return settings.model_dump()


@router_api.get("/settings/database/")
async def view_db_settings(
    user: UserOAuth = Depends(current_superuser_act),
):
    settings = Inject(get_db_settings)
    return settings.model_dump()


@router_api.get("/settings/email/")
async def view_email_settings(
    user: UserOAuth = Depends(current_superuser_act),
):
    settings = Inject(get_email_settings)
    return settings.model_dump()


@router_api.get("/settings/data/")
async def view_data_settings(
    user: UserOAuth = Depends(current_superuser_act),
):
    settings = Inject(get_data_settings)
    return settings.model_dump()


@router_api.get("/settings/oauth/")
async def view_oauth_settings(
    user: UserOAuth = Depends(current_superuser_act),
):
    settings = Inject(get_oauth_settings)
    return settings.model_dump()
