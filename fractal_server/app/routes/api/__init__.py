"""
`api` module
"""
from fastapi import APIRouter

from ....config import get_settings
from ....syringe import Inject


router_api = APIRouter()


@router_api.get("/alive/")
async def alive():
    settings = Inject(get_settings)
    return dict(
        alive=True,
        version=settings.PROJECT_VERSION,
    )
