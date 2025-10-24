from fastapi import Depends
from fastapi import HTTPException
from fastapi import status
from fastapi_users import FastAPIUsers
from fastapi_users.authentication import AuthenticationBackend
from fastapi_users.authentication import BearerTransport
from fastapi_users.authentication import CookieTransport
from fastapi_users.authentication import JWTStrategy

from fractal_server.app.models import UserOAuth
from fractal_server.app.security import get_user_manager
from fractal_server.config import get_settings
from fractal_server.syringe import Inject


bearer_transport = BearerTransport(tokenUrl="/auth/token/login")
cookie_transport = CookieTransport(cookie_samesite="none")


def get_jwt_strategy() -> JWTStrategy:
    settings = Inject(get_settings)
    return JWTStrategy(
        secret=settings.JWT_SECRET_KEY,  # type: ignore
        lifetime_seconds=settings.JWT_EXPIRE_SECONDS,
    )


def get_jwt_cookie_strategy() -> JWTStrategy:
    settings = Inject(get_settings)
    return JWTStrategy(
        secret=settings.JWT_SECRET_KEY,  # type: ignore
        lifetime_seconds=settings.COOKIE_EXPIRE_SECONDS,
    )


token_backend = AuthenticationBackend(
    name="bearer-jwt",
    transport=bearer_transport,
    get_strategy=get_jwt_strategy,
)
cookie_backend = AuthenticationBackend(
    name="cookie-jwt",
    transport=cookie_transport,
    get_strategy=get_jwt_cookie_strategy,
)


fastapi_users = FastAPIUsers[UserOAuth, int](
    get_user_manager,
    [token_backend, cookie_backend],
)

# Current-user dependencies
current_user_act = fastapi_users.current_user(active=True)
current_user_act_ver = fastapi_users.current_user(
    active=True,
    verified=True,
)


async def current_user_act_ver_prof(
    user: UserOAuth = Depends(current_user_act_ver),
) -> UserOAuth:
    """
    Require a active&verified user, with a non-null `profile_id`.

    Raises 401 if user does not exist or is not active.
    Raises 403 if user is not verified or has null `profile_id`.
    """
    if user.profile_id is None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                f"Forbidden access "
                f"({user.is_verified=} {user.profile_id=})."
            ),
        )
    return user


current_superuser_act = fastapi_users.current_user(
    active=True,
    superuser=True,
)
