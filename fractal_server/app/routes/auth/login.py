"""
Definition of basic-auth login/logout endpoints.
"""

from fastapi import APIRouter

from fractal_server.config import get_settings
from fractal_server.syringe import Inject

from . import fastapi_users
from . import token_backend
from ._aux_auth import _add_trailing_slash_in_place
from ._aux_auth import _remove_login_route_in_place


def get_token_login_router() -> APIRouter:
    """
    Get the `APIRouter` for `/auth/token/login/` and `/auth/token/logout/`
    """
    settings = Inject(get_settings)

    router_token_login = APIRouter()
    router_token_login.include_router(
        fastapi_users.get_auth_router(token_backend), prefix="/token"
    )
    if settings.FRACTAL_DISABLE_BASIC_AUTH == "true":
        _remove_login_route_in_place(router_token_login)

    _add_trailing_slash_in_place(router_token_login)
    return router_token_login
