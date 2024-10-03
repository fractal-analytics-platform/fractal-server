from fastapi import APIRouter

from .current_user import router_current_user
from .group import router_group
from .login import router_login
from .oauth import router_oauth
from .register import router_register
from .users import router_users

router_auth = APIRouter()

router_auth.include_router(router_register)
router_auth.include_router(router_current_user)
router_auth.include_router(router_login)
router_auth.include_router(router_users)
router_auth.include_router(router_group)
router_auth.include_router(router_oauth)
