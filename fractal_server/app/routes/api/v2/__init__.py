"""
`api/v1` module
"""
from fastapi import APIRouter

from .dataset import router as dataset_router_v2

router_api_v2 = APIRouter()

router_api_v2.include_router(dataset_router_v2, tags=["V2 Datasets"])
