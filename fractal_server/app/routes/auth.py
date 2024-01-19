"""
Definition of `/auth` routes.
"""
from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status
from fastapi_users import exceptions
from fastapi_users import schemas
from fastapi_users.router.common import ErrorCode
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from ...config import get_settings
from ...syringe import Inject
from ..db import get_async_db
from ..models.security import UserOAuth as User
from ..schemas.user import UserCreate
from ..schemas.user import UserRead
from ..schemas.user import UserUpdate
from ..schemas.user import UserUpdateStrict
from ..security import cookie_backend
from ..security import current_active_superuser
from ..security import current_active_user
from ..security import fastapi_users
from ..security import get_user_manager
from ..security import token_backend
from ..security import UserManager

router_auth = APIRouter()

router_auth.include_router(
    fastapi_users.get_auth_router(token_backend),
    prefix="/token",
)
router_auth.include_router(
    fastapi_users.get_auth_router(cookie_backend),
)
router_auth.include_router(
    fastapi_users.get_register_router(UserRead, UserCreate),
    dependencies=[Depends(current_active_superuser)],
)

users_router = fastapi_users.get_users_router(UserRead, UserUpdate)

# We remove `/auth/users/me` endpoints to implement our own
# at `/auth/current-user/`.
# We also remove `DELETE /auth/users/{user_id}`
# (ref https://github.com/fastapi-users/fastapi-users/discussions/606)
users_router.routes = [
    route
    for route in users_router.routes
    if route.name
    not in [
        "users:current_user",
        "users:delete_user",
        "users:patch_current_user",
    ]
]
router_auth.include_router(
    users_router,
    prefix="/users",
    dependencies=[Depends(current_active_superuser)],
)


@router_auth.patch("/current-user/", response_model=UserRead)
async def patch_current_user(
    user_update: UserUpdateStrict,
    current_user: User = Depends(current_active_user),
    user_manager: UserManager = Depends(get_user_manager),
):

    update = UserUpdate(**user_update.dict(exclude_unset=True))

    try:
        user = await user_manager.update(update, current_user, safe=True)
    except exceptions.InvalidPasswordException as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "code": ErrorCode.UPDATE_USER_INVALID_PASSWORD,
                "reason": e.reason,
            },
        )
    return schemas.model_validate(User, user)


@router_auth.get("/current-user/", response_model=UserRead)
async def get_current_user(user: User = Depends(current_active_user)):
    """
    Return current user
    """
    return user


@router_auth.get("/users/", response_model=list[UserRead])
async def list_users(
    user: User = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
):
    """
    Return list of all users
    """
    stm = select(User)
    res = await db.execute(stm)
    user_list = res.scalars().unique().all()
    await db.close()
    return user_list


# OAUTH CLIENTS

# NOTE: settings.OAUTH_CLIENTS are collected by
# Settings.collect_oauth_clients(). If no specific client is specified in the
# environment variables (e.g. by setting OAUTH_FOO_CLIENT_ID and
# OAUTH_FOO_CLIENT_SECRET), this list is empty

# FIXME:Dependency injection should be wrapped within a function call to make
# it truly lazy. This function could then be called on startup of the FastAPI
# app (cf. fractal_server.main)
settings = Inject(get_settings)

for client_config in settings.OAUTH_CLIENTS_CONFIG:

    client_name = client_config.CLIENT_NAME.lower()

    if client_name == "google":
        from httpx_oauth.clients.google import GoogleOAuth2

        client = GoogleOAuth2(
            client_config.CLIENT_ID, client_config.CLIENT_SECRET
        )
    elif client_name == "github":
        from httpx_oauth.clients.github import GitHubOAuth2

        client = GitHubOAuth2(
            client_config.CLIENT_ID, client_config.CLIENT_SECRET
        )
    else:
        from httpx_oauth.clients.openid import OpenID

        client = OpenID(
            client_config.CLIENT_ID,
            client_config.CLIENT_SECRET,
            client_config.OIDC_CONFIGURATION_ENDPOINT,
        )

    router_auth.include_router(
        fastapi_users.get_oauth_router(
            client,
            cookie_backend,
            settings.JWT_SECRET_KEY,
            is_verified_by_default=False,
            associate_by_email=True,
            redirect_url=client_config.REDIRECT_URL,
        ),
        prefix=f"/{client_name}",
    )


# Add trailing slash to all routes' paths
for route in router_auth.routes:
    if not route.path.endswith("/"):
        route.path = f"{route.path}/"
