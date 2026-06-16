"""
Definition of `/auth/register/` routes.
"""

from fastapi import APIRouter
from fastapi import Depends

from fractal_server.app.schemas.user import UserCreate
from fractal_server.app.schemas.user import UserRead

from . import current_superuser_act
from . import fastapi_users
from ._aux_auth import _add_trailing_slash_in_place

router_register = APIRouter()

_register_router = fastapi_users.get_register_router(UserRead, UserCreate)
_add_trailing_slash_in_place(_register_router)
router_register.include_router(
    _register_router, dependencies=[Depends(current_superuser_act)]
)
