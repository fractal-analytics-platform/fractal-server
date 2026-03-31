from fastapi import APIRouter

from .current_user import router_current_user
from .group import router_group
from .login import get_login_router
from .oauth import get_oauth_router
from .register import router_register
from .users import router_users
from .viewer_paths import router_viewer_paths

router_auth = APIRouter()

router_auth.include_router(router_register)
router_auth.include_router(router_current_user)
router_auth.include_router(get_login_router())
router_auth.include_router(router_users)
router_auth.include_router(router_group)
router_auth.include_router(router_viewer_paths)
router_oauth = get_oauth_router()
if router_oauth is not None:
    router_auth.include_router(router_oauth)
