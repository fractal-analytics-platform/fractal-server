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
import contextlib
from typing import Any
from typing import AsyncGenerator
from typing import Dict
from typing import Generic
from typing import Optional
from typing import Type

from fastapi import Depends
from fastapi_users import BaseUserManager
from fastapi_users import FastAPIUsers
from fastapi_users import IntegerIDMixin
from fastapi_users.authentication import AuthenticationBackend
from fastapi_users.authentication import BearerTransport
from fastapi_users.authentication import CookieTransport
from fastapi_users.authentication import JWTStrategy
from fastapi_users.db.base import BaseUserDatabase
from fastapi_users.exceptions import InvalidPasswordException
from fastapi_users.exceptions import UserAlreadyExists
from fastapi_users.models import ID
from fastapi_users.models import OAP
from fastapi_users.models import UP
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlmodel import func
from sqlmodel import select

from ...config import get_settings
from ...syringe import Inject
from ..db import get_async_db
from fractal_server.app.models.security import OAuthAccount
from fractal_server.app.models.security import UserOAuth as User
from fractal_server.app.schemas.user import UserCreate
from fractal_server.logger import get_logger

logger = get_logger(__name__)


class SQLModelUserDatabaseAsync(Generic[UP, ID], BaseUserDatabase[UP, ID]):
    """
    This class is from fastapi_users_db_sqlmodel
    Original Copyright: 2022 François Voron, released under MIT licence

    Database adapter for SQLModel working purely asynchronously.

    Args:
        user_model: SQLModel model of a DB representation of a user.
        session: SQLAlchemy async session.
    """

    session: AsyncSession
    user_model: Type[UP]
    oauth_account_model: Optional[Type[OAuthAccount]]

    def __init__(
        self,
        session: AsyncSession,
        user_model: Type[UP],
        oauth_account_model: Optional[Type[OAuthAccount]] = None,
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
    session: AsyncSession = Depends(get_async_db),
) -> AsyncGenerator[SQLModelUserDatabaseAsync, None]:
    yield SQLModelUserDatabaseAsync(session, User, OAuthAccount)


class UserManager(IntegerIDMixin, BaseUserManager[User, int]):
    async def validate_password(self, password: str, user: User) -> None:
        # check password length
        min_length, max_length = 4, 100
        if len(password) < min_length:
            raise InvalidPasswordException(
                f"The password is too short (minimum length: {min_length})."
            )
        elif len(password) > max_length:
            raise InvalidPasswordException(
                f"The password is too long (maximum length: {min_length})."
            )


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


# Create dependencies for users
current_active_user = fastapi_users.current_user(active=True)
current_active_verified_user = fastapi_users.current_user(
    active=True, verified=True
)
current_active_superuser = fastapi_users.current_user(
    active=True, superuser=True
)

get_async_session_context = contextlib.asynccontextmanager(get_async_db)
get_user_db_context = contextlib.asynccontextmanager(get_user_db)
get_user_manager_context = contextlib.asynccontextmanager(get_user_manager)


async def _create_first_user(
    email: str,
    password: str,
    is_superuser: bool = False,
    is_verified: bool = False,
    username: Optional[str] = None,
) -> None:
    """
    Private method to create the first fractal-server user

    Create a user with the given default arguments and return a message with
    the relevant informations. If the user alredy exists, for example after a
    restart, it returns a message to inform that user already exists.

    **WARNING**: This function is only meant to create the first user, and then
    it catches and ignores `IntegrityError`s (when multiple workers may be
    trying to concurrently create the first user). This is not the expected
    behavior for regular user creation, which must rather happen via the
    /auth/register endpoint.

    See [fastapi_users docs](https://fastapi-users.github.io/fastapi-users/
    12.1/cookbook/create-user-programmatically)

    Arguments:
        email: New user's email
        password: New user's password
        is_superuser: `True` if the new user is a superuser
        is_verified: `True` if the new user is verifie
        username:
    """
    try:
        async with get_async_session_context() as session:

            if is_superuser is True:
                # If a superuser already exists, exit
                stm = select(User).where(
                    User.is_superuser == True  # noqa E712
                )
                res = await session.execute(stm)
                existing_superuser = res.scalars().first()
                if existing_superuser is not None:
                    logger.info(
                        f"{existing_superuser.email} superuser already exists,"
                        f" skip creation of {email}"
                    )
                    return None

            async with get_user_db_context(session) as user_db:
                async with get_user_manager_context(user_db) as user_manager:
                    kwargs = dict(
                        email=email,
                        password=password,
                        is_superuser=is_superuser,
                        is_verified=is_verified,
                    )
                    if username is not None:
                        kwargs["username"] = username
                    user = await user_manager.create(UserCreate(**kwargs))
                    logger.info(f"User {user.email} created")

    except IntegrityError:
        logger.warning(
            f"Creation of user {email} failed with IntegrityError "
            "(likely due to concurrent attempts from different workers)."
        )

    except UserAlreadyExists:
        logger.warning(f"User {email} already exists")
