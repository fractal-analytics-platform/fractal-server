"""
`api/v2` module
"""
from fastapi import APIRouter

from .dataset import router as dataset_router_v2
from .images import router as images_routes_v2
from .job import router as job_router_v2
from .project import router as project_router_v2
from .status import router as status_router_v2
from .submit import router as submit_job_router_v2
from .task import router as task_router_v2
from .task_collection import router as task_collection_router_v2
from .task_collection_custom import router as task_collection_router_v2_custom
from .task_group import router as task_group_router_v2
from .task_group_lifecycle import router as task_group_lifecycle_router_v2
from .workflow import router as workflow_router_v2
from .workflow_import import router as workflow_import_router_v2
from .workflowtask import router as workflowtask_router_v2
from fractal_server.config import get_settings
from fractal_server.syringe import Inject


router_api_v2 = APIRouter()

router_api_v2.include_router(dataset_router_v2, tags=["V2 Dataset"])
router_api_v2.include_router(job_router_v2, tags=["V2 Job"])
router_api_v2.include_router(images_routes_v2, tags=["V2 Images"])
router_api_v2.include_router(project_router_v2, tags=["V2 Project"])
router_api_v2.include_router(submit_job_router_v2, tags=["V2 Job"])


settings = Inject(get_settings)
router_api_v2.include_router(
    task_collection_router_v2,
    prefix="/task",
    tags=["V2 Task Lifecycle"],
)
router_api_v2.include_router(
    task_collection_router_v2_custom,
    prefix="/task",
    tags=["V2 Task Lifecycle"],
)
router_api_v2.include_router(
    task_group_lifecycle_router_v2,
    prefix="/task-group",
    tags=["V2 Task Lifecycle"],
)

router_api_v2.include_router(task_router_v2, prefix="/task", tags=["V2 Task"])
router_api_v2.include_router(
    task_group_router_v2, prefix="/task-group", tags=["V2 TaskGroup"]
)
router_api_v2.include_router(workflow_router_v2, tags=["V2 Workflow"])
router_api_v2.include_router(
    workflow_import_router_v2, tags=["V2 Workflow Import"]
)
router_api_v2.include_router(workflowtask_router_v2, tags=["V2 WorkflowTask"])
router_api_v2.include_router(status_router_v2, tags=["V2 Status"])
