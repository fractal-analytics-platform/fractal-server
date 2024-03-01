"""
`api/v1` module
"""
from fastapi import APIRouter

from .dataset import router as dataset_router_v2
from .project import router as project_router_v2
from .runner import router as runner_router_v2
from .workflow import router as workflow_router_v2

router_api_v2 = APIRouter()

router_api_v2.include_router(dataset_router_v2, tags=["Dataset V2"])
router_api_v2.include_router(project_router_v2, tags=["Project V2"])
router_api_v2.include_router(runner_router_v2, tags=["Runner V2"])
router_api_v2.include_router(workflow_router_v2, tags=["Workflow V2"])
