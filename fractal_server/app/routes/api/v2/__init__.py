"""
`api/v2` module
"""
from fastapi import APIRouter

from .dataset import router as dataset_router_v2
from .images import router as images_routes_v2
from .job import router as job_router_v2
from .project import router as project_router_v2
from .submit import router as submit_job_router_v2
from .task import router as task_router_v2
from .task_collection import router as task_collection_router_v2
from .workflow import router as workflow_router_v2
from .workflowtask import router as workflowtask_router_v2

router_api_v2 = APIRouter()

router_api_v2.include_router(dataset_router_v2, tags=["V2 Dataset"])
router_api_v2.include_router(job_router_v2, tags=["V2 Job"])
router_api_v2.include_router(images_routes_v2, tags=["V2 Images"])
router_api_v2.include_router(project_router_v2, tags=["V2 Project"])
router_api_v2.include_router(submit_job_router_v2, tags=["V2 Submit Job"])
router_api_v2.include_router(task_router_v2, prefix="/task", tags=["V2 Task"])
router_api_v2.include_router(
    task_collection_router_v2, prefix="/task", tags=["V2 Task Collection"]
)
router_api_v2.include_router(workflow_router_v2, tags=["V2 Workflow"])
router_api_v2.include_router(workflowtask_router_v2, tags=["V2 WorkflowTask"])
