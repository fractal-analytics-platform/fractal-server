"""
`api/v1` module
"""
from fastapi import APIRouter

from .apply import router as runner_router_v2
from .dataset import router as dataset_router_v2
from .job import router as job_router_v2
from .project import router as project_router_v2
from .workflow import router as workflow_router_v2


router_api_v2 = APIRouter()

router_api_v2.include_router(dataset_router_v2, tags=["V2 Dataset"])
router_api_v2.include_router(project_router_v2, tags=["V2 Project"])
router_api_v2.include_router(runner_router_v2, tags=["V2 Apply-workflow"])
router_api_v2.include_router(workflow_router_v2, tags=["V2 Workflow"])
router_api_v2.include_router(job_router_v2, tags=["V2 Job"])
