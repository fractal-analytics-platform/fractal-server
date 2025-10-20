from fastapi import APIRouter

from .current_user import router_current_user
from .group import router_group
from .login import router_login
from .oauth import get_oauth_router
from .register import router_register
from .users import router_users

router_auth = APIRouter()

router_auth.include_router(router_register)
router_auth.include_router(router_current_user)
router_auth.include_router(router_login)
router_auth.include_router(router_users)
router_auth.include_router(router_group)
router_oauth = get_oauth_router()
if router_oauth is not None:
    router_auth.include_router(router_oauth)
