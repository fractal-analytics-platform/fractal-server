"""
Definition of `/auth/register/` routes.
"""

from fastapi import APIRouter
from fastapi import Depends

from fractal_server.app.schemas.user import UserCreate
from fractal_server.app.schemas.user import UserRead

from . import current_superuser_act
from . import fastapi_users

router_register = APIRouter()

router_register.include_router(
    fastapi_users.get_register_router(UserRead, UserCreate),
    dependencies=[Depends(current_superuser_act)],
)


# Add trailing slash to all routes' paths
for route in router_register.routes:
    if not route.path.endswith("/"):
        route.path = f"{route.path}/"
