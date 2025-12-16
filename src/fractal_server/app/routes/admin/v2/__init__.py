"""
`admin/v2` module
"""

from fastapi import APIRouter

from .accounting import router as accounting_router
from .impersonate import router as impersonate_router
from .job import router as job_router
from .profile import router as profile_router
from .resource import router as resource_router
from .sharing import router as sharing_router
from .task import router as task_router
from .task_group import router as task_group_router
from .task_group_lifecycle import router as task_group_lifecycle_router

router_admin = APIRouter()

router_admin.include_router(accounting_router, prefix="/accounting")
router_admin.include_router(job_router, prefix="/job")
router_admin.include_router(task_router, prefix="/task")
router_admin.include_router(task_group_router, prefix="/task-group")
router_admin.include_router(task_group_lifecycle_router, prefix="/task-group")
router_admin.include_router(impersonate_router, prefix="/impersonate")
router_admin.include_router(resource_router, prefix="/resource")
router_admin.include_router(profile_router, prefix="/profile")
router_admin.include_router(sharing_router, prefix="/linkuserproject")
