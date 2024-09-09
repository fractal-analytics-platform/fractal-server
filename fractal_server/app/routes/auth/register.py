"""
Definition of `/auth/register/` routes.
"""
from fastapi import APIRouter
from fastapi import Depends

from . import current_active_superuser
from . import fastapi_users
from ...schemas.user import UserCreate
from ...schemas.user import UserRead

router_register = APIRouter()

router_register.include_router(
    fastapi_users.get_register_router(UserRead, UserCreate),
    dependencies=[Depends(current_active_superuser)],
)


# Add trailing slash to all routes' paths
for route in router_register.routes:
    if not route.path.endswith("/"):
        route.path = f"{route.path}/"
