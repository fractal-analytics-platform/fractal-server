"""
`admin/v2` module
"""
from fastapi import APIRouter

from .job import router as job_router
from .project import router as project_router
from .task import router as task_router
from .task_group import router as task_group_router
from .task_group_lifecycle import router as task_group_lifecycle_router

router_admin_v2 = APIRouter()

router_admin_v2.include_router(job_router, prefix="/job")
router_admin_v2.include_router(project_router, prefix="/project")
router_admin_v2.include_router(task_router, prefix="/task")
router_admin_v2.include_router(task_group_router, prefix="/task-group")
router_admin_v2.include_router(
    task_group_lifecycle_router, prefix="/task-group"
)
