# Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
# University of Zurich
#
# Original authors:
# Jacopo Nespolo <jacopo.nespolo@exact-lab.it>
# Tommaso Comparin <tommaso.comparin@exact-lab.it>
#
# This file is part of Fractal and was originally developed by eXact lab S.r.l.
# <exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
# Institute for Biomedical Research and Pelkmans Lab from the University of
# Zurich.
"""
Auth subsystem

This module implements the authorisation/authentication subsystem of the
Fractal Server. It is based on the
[FastAPI Users](https://fastapi-users.github.io/fastapi-users/)
library with
[support](https://github.com/fastapi-users/fastapi-users-db-sqlmodel) for the
SQLModel database adapter.

In particular, this module links the appropriate database models, sets up
FastAPIUsers with Barer Token and cookie transports and register local routes.
Then, for each OAuth client defined in the Fractal Settings configuration, it
registers the client and the relative routes.

All routes are registerd under the `auth/` prefix.
"""
from typing import Any
from typing import AsyncGenerator
from typing import Dict
from typing import Generic
from typing import Optional
from typing import Type
from typing import Union

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status
from fastapi_users import BaseUserManager
from fastapi_users import FastAPIUsers
from fastapi_users import IntegerIDMixin
from fastapi_users.authentication import AuthenticationBackend
from fastapi_users.authentication import BearerTransport
from fastapi_users.authentication import CookieTransport
from fastapi_users.authentication import JWTStrategy
from fastapi_users.db.base import BaseUserDatabase
from fastapi_users.models import ID
from fastapi_users.models import OAP
from fastapi_users.models import UP
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlmodel import func
from sqlmodel import select

from ...common.schemas.user import UserCreate
from ...common.schemas.user import UserRead
from ...common.schemas.user import UserUpdate
from ...config import get_settings
from ...syringe import Inject
from ..db import get_db
from ..models.security import OAuthAccount
from ..models.security import SQLModelBaseOAuthAccount
from ..models.security import UserOAuth as User


class SQLModelUserDatabaseAsync(Generic[UP, ID], BaseUserDatabase[UP, ID]):
    """
    This class is from fastapi_users_db_sqlmodel
    Original Copyright: 2022 François Voron, released under MIT licence

    Database adapter for SQLModel working purely asynchronously.
    :param user_model: SQLModel model of a DB representation of a user.
    :param session: SQLAlchemy async session.
    """

    session: AsyncSession
    user_model: Type[UP]
    oauth_account_model: Optional[Type[SQLModelBaseOAuthAccount]]

    def __init__(
        self,
        session: AsyncSession,
        user_model: Type[UP],
        oauth_account_model: Optional[Type[SQLModelBaseOAuthAccount]] = None,
    ):
        self.session = session
        self.user_model = user_model
        self.oauth_account_model = oauth_account_model

    async def get(self, id: ID) -> Optional[UP]:
        """Get a single user by id."""
        return await self.session.get(self.user_model, id)

    async def get_by_email(self, email: str) -> Optional[UP]:
        """Get a single user by email."""
        statement = select(self.user_model).where(
            func.lower(self.user_model.email) == func.lower(email)
        )
        results = await self.session.execute(statement)
        object = results.first()
        if object is None:
            return None
        return object[0]

    async def get_by_oauth_account(
        self, oauth: str, account_id: str
    ) -> Optional[UP]:  # noqa
        """Get a single user by OAuth account id."""
        if self.oauth_account_model is None:
            raise NotImplementedError()
        statement = (
            select(self.oauth_account_model)
            .where(self.oauth_account_model.oauth_name == oauth)
            .where(self.oauth_account_model.account_id == account_id)
            .options(selectinload(self.oauth_account_model.user))  # type: ignore  # noqa
        )
        results = await self.session.execute(statement)
        oauth_account = results.first()
        if oauth_account:
            user = oauth_account[0].user  # type: ignore
            return user
        return None

    async def create(self, create_dict: Dict[str, Any]) -> UP:
        """Create a user."""
        user = self.user_model(**create_dict)
        self.session.add(user)
        await self.session.commit()
        await self.session.refresh(user)
        return user

    async def update(self, user: UP, update_dict: Dict[str, Any]) -> UP:
        for key, value in update_dict.items():
            setattr(user, key, value)
        self.session.add(user)
        await self.session.commit()
        await self.session.refresh(user)
        return user

    async def delete(self, user: UP) -> None:
        await self.session.delete(user)
        await self.session.commit()

    async def add_oauth_account(
        self, user: UP, create_dict: Dict[str, Any]
    ) -> UP:  # noqa
        if self.oauth_account_model is None:
            raise NotImplementedError()

        oauth_account = self.oauth_account_model(**create_dict)
        user.oauth_accounts.append(oauth_account)  # type: ignore
        self.session.add(user)

        await self.session.commit()

        return user

    async def update_oauth_account(
        self, user: UP, oauth_account: OAP, update_dict: Dict[str, Any]
    ) -> UP:
        if self.oauth_account_model is None:
            raise NotImplementedError()

        for key, value in update_dict.items():
            setattr(oauth_account, key, value)
        self.session.add(oauth_account)
        await self.session.commit()

        return user


async def get_user_db(
    session: AsyncSession = Depends(get_db),
) -> AsyncGenerator[SQLModelUserDatabaseAsync, None]:
    yield SQLModelUserDatabaseAsync(session, User, OAuthAccount)


class UserManager(IntegerIDMixin, BaseUserManager[User, int]):
    pass


async def get_user_manager(
    user_db: SQLModelUserDatabaseAsync = Depends(get_user_db),
) -> AsyncGenerator[UserManager, None]:
    yield UserManager(user_db)


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


fastapi_users = FastAPIUsers[User, int](
    get_user_manager,
    [token_backend, cookie_backend],
)


# Create dependencies for active user and for superuser
current_active_user = fastapi_users.current_user(active=True)


async def current_active_superuser(user=Depends(current_active_user)):
    # See https://github.com/fastapi-users/fastapi-users/discussions/454
    if not user.is_superuser:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This action is restricted to superusers",
        )
    return user


# AUTH ROUTES

auth_router = APIRouter()

auth_router.include_router(
    fastapi_users.get_auth_router(token_backend),
    prefix="/token",
)
auth_router.include_router(
    fastapi_users.get_auth_router(cookie_backend),
)
auth_router.include_router(
    fastapi_users.get_register_router(UserRead, UserCreate),
    dependencies=[Depends(current_active_superuser)],
)
auth_router.include_router(
    fastapi_users.get_reset_password_router(),
)
auth_router.include_router(
    fastapi_users.get_verify_router(UserRead),
)
auth_router.include_router(
    fastapi_users.get_users_router(UserRead, UserUpdate),
    prefix="/users",
    dependencies=[Depends(current_active_superuser)],
)


@auth_router.get("/whoami", response_model=UserRead)
async def whoami(
    user: User = Depends(current_active_user),
):
    """
    Return current user
    """
    return user


@auth_router.get("/userlist", response_model=list[UserRead])
async def list_users(
    user: User = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_db),
):
    """
    Return list of all users
    """
    stm = select(User)
    res = await db.execute(stm)
    user_list = res.scalars().all()
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

for client in settings.OAUTH_CLIENTS:
    # INIT CLIENTS
    client_name = client.CLIENT_NAME.lower()
    _client: Optional[Union["GitHubOAuth2", "OAuth2"]] = None
    if client_name == "github":
        from httpx_oauth.clients.github import GitHubOAuth2

        _client = GitHubOAuth2(client.CLIENT_ID, client.CLIENT_SECRET)
    else:  # GENERIC CLIENT
        from httpx_oauth.oauth2 import OAuth2

        if (
            not client.CLIENT_SECRET
            or not client.AUTHORIZE_ENDPOINT
            or not client.ACCESS_TOKEN_ENDPOINT
        ):
            raise ValueError(
                "Must specify CLIENT_SECRET, AUTHORIZE_ENDPOINT and "
                "ACCESS_TOKEN_ENDPOINT to define custom OAuth2 client."
            )
        _client = OAuth2(
            client.CLIENT_ID,
            client.CLIENT_SECRET,
            client.AUTHORIZE_ENDPOINT,
            client.ACCESS_TOKEN_ENDPOINT,
            refresh_token_endpoint=client.REFRESH_TOKEN_ENDPOINT,
            revoke_token_endpoint=client.REVOKE_TOKEN_ENDPOINT,
        )

    # ADD ROUTES
    # GitHub OAuth
    auth_router.include_router(
        fastapi_users.get_oauth_router(
            _client,
            cookie_backend,
            settings.JWT_SECRET_KEY,  # type: ignore
            # WARNING:
            # associate_by_email=True exposes to security risks if the OAuth
            # provider does not verify emails.
            associate_by_email=True,
        ),
        prefix=f"/{client_name}",
    )
    auth_router.include_router(
        fastapi_users.get_oauth_associate_router(
            _client, UserRead, settings.JWT_SECRET_KEY  # type: ignore
        ),
        prefix=f"/{client_name}/associate",
    )
