from fastapi import APIRouter

from .alive import router as router_alive
from .settings import router as router_settings

router_api = APIRouter()

router_api.include_router(router_alive)
router_api.include_router(router_settings)
