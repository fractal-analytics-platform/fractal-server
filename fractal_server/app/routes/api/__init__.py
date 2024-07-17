"""
`api` module
"""
from fastapi import APIRouter
from fastapi import Depends

from ....config import get_settings
from ....syringe import Inject
from ...models.security import UserOAuth
from ...security import current_active_superuser


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
    for k in settings.dict():
        if ("PASSWORD" in k) or ("SECRET" in k):
            setattr(settings, k, "***")
    return settings
