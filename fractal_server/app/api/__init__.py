"""
`api` module
"""
from fastapi import APIRouter

from ...config import get_settings
from ...syringe import Inject
from .v1.job import router as job_router
from .v1.project import router as project_router
from .v1.task import router as task_router
from .v1.workflow import router as workflow_router


router_default = APIRouter()
router_v1 = APIRouter()

router_v1.include_router(project_router, prefix="/project", tags=["project"])
router_v1.include_router(task_router, prefix="/task", tags=["task"])
router_v1.include_router(
    workflow_router, prefix="/workflow", tags=["workflow"]
)
router_v1.include_router(job_router, prefix="/job", tags=["monitoring"])


@router_default.get("/alive/")
async def alive():
    settings = Inject(get_settings)
    return dict(
        alive=True,
        deployment_type=settings.DEPLOYMENT_TYPE,
        version=settings.PROJECT_VERSION,
    )
