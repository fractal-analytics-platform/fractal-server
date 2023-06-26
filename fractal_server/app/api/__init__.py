"""
`api` module
"""
from fastapi import APIRouter

from ...config import get_settings
from ...syringe import Inject
from .v1.dataset import router as dataset_router
from .v1.job import router as job_router
from .v1.project import router as project_router
from .v1.task import router as task_router
from .v1.task_collection import router as taskcollection_router
from .v1.workflow import router as workflow_router
from .v1.workflowtask import router as workflowtask_router


router_default = APIRouter()
router_v1 = APIRouter()

router_v1.include_router(project_router, prefix="/project", tags=["Projects"])
router_v1.include_router(task_router, prefix="/task", tags=["Tasks"])
router_v1.include_router(
    taskcollection_router, prefix="/task", tags=["Task Collection"]
)
router_v1.include_router(dataset_router, tags=["Datasets"])
router_v1.include_router(workflow_router, tags=["Workflows"])
router_v1.include_router(workflowtask_router, tags=["Workflow Tasks"])
router_v1.include_router(job_router, tags=["Jobs"])


@router_default.get("/alive/")
async def alive():
    settings = Inject(get_settings)
    return dict(
        alive=True,
        deployment_type=settings.DEPLOYMENT_TYPE,
        version=settings.PROJECT_VERSION,
    )
