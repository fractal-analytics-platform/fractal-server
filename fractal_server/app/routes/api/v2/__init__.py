"""
`api/v2` module
"""

from fastapi import APIRouter

from fractal_server.config import get_settings
from fractal_server.syringe import Inject

from .dataset import router as dataset_router
from .history import router as history_router
from .images import router as images_routes
from .job import router as job_router
from .pre_submission_checks import router as pre_submission_checks_router
from .project import router as project_router
from .sharing import router as sharing_router
from .status_legacy import router as status_legacy_router
from .submit import router as submit_job_router
from .task import router as task_router
from .task_collection import router as task_collection_router
from .task_collection_custom import router as task_collection_router_custom
from .task_collection_pixi import router as task_collection_pixi_router
from .task_group import router as task_group_router
from .task_group_lifecycle import router as task_group_lifecycle_router
from .task_version_update import router as task_version_update_router
from .workflow import router as workflow_router
from .workflow_import import router as workflow_import_router
from .workflowtask import router as workflowtask_router

router_api = APIRouter()

router_api.include_router(dataset_router, tags=["Dataset"])
router_api.include_router(pre_submission_checks_router, tags=["Job"])
router_api.include_router(job_router, tags=["Job"])
router_api.include_router(images_routes, tags=["Images"])
router_api.include_router(sharing_router, tags=["Project Sharing"])
router_api.include_router(project_router, tags=["Project"])
router_api.include_router(submit_job_router, tags=["Job"])
router_api.include_router(history_router, tags=["History"])
router_api.include_router(status_legacy_router, tags=["Status Legacy"])


settings = Inject(get_settings)
router_api.include_router(
    task_collection_router,
    prefix="/task",
    tags=["Task Lifecycle"],
)
router_api.include_router(
    task_collection_router_custom,
    prefix="/task",
    tags=["Task Lifecycle"],
)
router_api.include_router(
    task_collection_pixi_router,
    prefix="/task",
    tags=["Task Lifecycle"],
)
router_api.include_router(
    task_group_lifecycle_router,
    prefix="/task-group",
    tags=["Task Lifecycle"],
)

router_api.include_router(task_router, prefix="/task", tags=["Task"])
router_api.include_router(task_version_update_router, tags=["Task"])
router_api.include_router(
    task_group_router, prefix="/task-group", tags=["TaskGroup"]
)
router_api.include_router(workflow_router, tags=["Workflow"])
router_api.include_router(workflow_import_router, tags=["Workflow Import"])
router_api.include_router(workflowtask_router, tags=["WorkflowTask"])
