"""
Definition of `/auth/{login,logout}/`, `/auth/token/{login/logout}` routes.
"""
from fastapi import APIRouter

from . import cookie_backend
from . import fastapi_users
from . import token_backend

router_login = APIRouter()


router_login.include_router(
    fastapi_users.get_auth_router(token_backend),
    prefix="/token",
)
router_login.include_router(
    fastapi_users.get_auth_router(cookie_backend),
)


# Add trailing slash to all routes paths
for route in router_login.routes:
    if not route.path.endswith("/"):
        route.path = f"{route.path}/"
