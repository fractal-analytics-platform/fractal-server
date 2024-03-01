"""
`api/v1` module
"""
from fastapi import APIRouter

from .dataset import router as dataset_router
from .job import router as job_router
from .project import router as project_router
from .task import router as task_router
from .task_collection import router as taskcollection_router
from .workflow import router as workflow_router
from .workflowtask import router as workflowtask_router

router_api_v1 = APIRouter()

router_api_v1.include_router(
    project_router, prefix="/project", tags=["V1 Project"]
)
router_api_v1.include_router(task_router, prefix="/task", tags=["V1 Task"])
router_api_v1.include_router(
    taskcollection_router, prefix="/task", tags=["V1 Task Collection"]
)
router_api_v1.include_router(dataset_router, tags=["V1 Dataset"])
router_api_v1.include_router(workflow_router, tags=["V1 Workflow"])
router_api_v1.include_router(workflowtask_router, tags=["V1 WorkflowTask"])
router_api_v1.include_router(job_router, tags=["V1 Job"])
