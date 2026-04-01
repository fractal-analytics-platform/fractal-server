"""
Definition of basic-auth login/logout endpoints.
"""

from fastapi import APIRouter

from fractal_server.config import get_settings
from fractal_server.syringe import Inject

from . import fastapi_users
from . import token_backend

router_login = APIRouter()


def get_login_router() -> APIRouter:
    """
    Get the `APIRouter` for `/auth/token/login/` and `/auth/token/logout/`
    """
    settings = Inject(get_settings)
    router_login = APIRouter()
    router_login.include_router(
        fastapi_users.get_auth_router(token_backend),
        prefix="/token",
    )
    if settings.FRACTAL_DISABLE_BASIC_AUTH == "true":
        # Remove `/auth/token/login/`
        original_routes = router_login.routes[:]
        router_login.routes = [
            route
            for route in original_routes
            if not route.path.startswith("/token/login")
        ]

    # Add trailing slash to all routes paths
    for route in router_login.routes:
        if not route.path.endswith("/"):
            route.path = f"{route.path}/"

    return router_login
