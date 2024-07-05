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
from .task_collection_ssh import router as task_collection_router_v2_ssh
from .task_legacy import router as task_legacy_router_v2
from .workflow import router as workflow_router_v2
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
if settings.FRACTAL_RUNNER_BACKEND == "slurm_ssh":
    router_api_v2.include_router(
        task_collection_router_v2_ssh,
        prefix="/task",
        tags=["V2 Task Collection"],
    )
else:
    router_api_v2.include_router(
        task_collection_router_v2, prefix="/task", tags=["V2 Task Collection"]
    )
    router_api_v2.include_router(
        task_collection_router_v2_custom,
        prefix="/task",
        tags=["V2 Task Collection"],
    )
router_api_v2.include_router(task_router_v2, prefix="/task", tags=["V2 Task"])
router_api_v2.include_router(
    task_legacy_router_v2, prefix="/task-legacy", tags=["V2 Task Legacy"]
)
router_api_v2.include_router(workflow_router_v2, tags=["V2 Workflow"])
router_api_v2.include_router(workflowtask_router_v2, tags=["V2 WorkflowTask"])
router_api_v2.include_router(status_router_v2, tags=["V2 Status"])
