"""
Definition of basic-auth login/logout endpoints.
"""

from fastapi import APIRouter

from fractal_server.config import get_settings
from fractal_server.syringe import Inject

from . import fastapi_users
from . import token_backend
from ._aux_auth import _add_trailing_slash_in_place


def get_token_login_router() -> APIRouter:
    """
    Get the `APIRouter` for `/auth/token/login/` and `/auth/token/logout/`

    NOTE: As of fastapi 0.137, the `routes` attribute may be a list of
    `_IncludedRouter` objects (instead of `BaseRoute`), which do not have a
    `path` attribute. This makes it more cumbersome to remove an item from the
    list. For this reason we create an intermediate `_tmp_router` object, with
    has the pre-0.137 property of `routes` being a list of `BaseRoute` - which
    is easier to edit (if `FRACTAL_DISABLE_BASIC_AUTH` is set).
    """
    settings = Inject(get_settings)

    _tmp_router = fastapi_users.get_auth_router(token_backend)

    if settings.FRACTAL_DISABLE_BASIC_AUTH == "true":
        original_routes = _tmp_router.routes[:]
        _tmp_router.routes = [
            route
            for route in original_routes
            if not route.path.startswith("/login")
        ]

    router_token_login = APIRouter()
    router_token_login.include_router(_tmp_router, prefix="/token")

    _add_trailing_slash_in_place(router_token_login)

    return router_token_login
