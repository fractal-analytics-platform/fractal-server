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

All routes are registered under the `auth/` prefix.
"""
import contextlib
from collections.abc import AsyncGenerator
from typing import Any
from typing import Generic

from fastapi import Depends
from fastapi import Request
from fastapi_users import BaseUserManager
from fastapi_users import IntegerIDMixin
from fastapi_users.db.base import BaseUserDatabase
from fastapi_users.exceptions import InvalidPasswordException
from fastapi_users.exceptions import UserAlreadyExists
from fastapi_users.models import ID
from fastapi_users.models import OAP
from fastapi_users.models import UP
from fastapi_users.password import PasswordHelper
from pwdlib import PasswordHash
from pwdlib.hashers.bcrypt import BcryptHasher
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlmodel import func
from sqlmodel import select

from ..db import get_async_db
from fractal_server.app.db import get_sync_db
from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import OAuthAccount
from fractal_server.app.models import UserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.models import UserSettings
from fractal_server.app.schemas.user import UserCreate
from fractal_server.app.security.signup_email import mail_new_oauth_signup
from fractal_server.config import get_settings
from fractal_server.logger import set_logger
from fractal_server.syringe import Inject

logger = set_logger(__name__)

FRACTAL_DEFAULT_GROUP_NAME = "All"


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
    user_model: type[UP]
    oauth_account_model: type[OAuthAccount] | None = None

    def __init__(
        self,
        session: AsyncSession,
        user_model: type[UP],
        oauth_account_model: type[OAuthAccount] | None = None,
    ):
        self.session = session
        self.user_model = user_model
        self.oauth_account_model = oauth_account_model

    async def get(self, id: ID) -> UP | None:
        """Get a single user by id."""
        return await self.session.get(self.user_model, id)

    async def get_by_email(self, email: str) -> UP | None:
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
    ) -> UP | None:  # noqa
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

    async def create(self, create_dict: dict[str, Any]) -> UP:
        """Create a user."""
        user = self.user_model(**create_dict)
        self.session.add(user)
        await self.session.commit()
        await self.session.refresh(user)
        return user

    async def update(self, user: UP, update_dict: dict[str, Any]) -> UP:
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
        self, user: UP, create_dict: dict[str, Any]
    ) -> UP:  # noqa
        if self.oauth_account_model is None:
            raise NotImplementedError()

        oauth_account = self.oauth_account_model(**create_dict)
        user.oauth_accounts.append(oauth_account)  # type: ignore
        self.session.add(user)

        await self.session.commit()

        return user

    async def update_oauth_account(
        self, user: UP, oauth_account: OAP, update_dict: dict[str, Any]
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
    yield SQLModelUserDatabaseAsync(session, UserOAuth, OAuthAccount)


password_hash = PasswordHash(hashers=(BcryptHasher(),))
password_helper = PasswordHelper(password_hash=password_hash)


class UserManager(IntegerIDMixin, BaseUserManager[UserOAuth, int]):
    def __init__(self, user_db):
        """
        Override `__init__` of `BaseUserManager` to define custom
        `password_helper`.
        """
        super().__init__(
            user_db=user_db,
            password_helper=password_helper,
        )

    async def validate_password(self, password: str, user: UserOAuth) -> None:
        # check password length
        min_length = 4
        max_length = 100
        if len(password) < min_length:
            raise InvalidPasswordException(
                f"The password is too short (minimum length: {min_length})."
            )
        elif len(password) > max_length:
            raise InvalidPasswordException(
                f"The password is too long (maximum length: {min_length})."
            )

    async def on_after_register(
        self, user: UserOAuth, request: Request | None = None
    ):
        logger.info(
            f"New-user registration completed ({user.id=}, {user.email=})."
        )
        async for db in get_async_db():
            # Find default group
            stm = select(UserGroup).where(
                UserGroup.name == FRACTAL_DEFAULT_GROUP_NAME
            )
            res = await db.execute(stm)
            default_group = res.scalar_one_or_none()
            if default_group is None:
                logger.warning(
                    f"No group found with name {FRACTAL_DEFAULT_GROUP_NAME}"
                )
            else:
                link = LinkUserGroup(
                    user_id=user.id, group_id=default_group.id
                )
                db.add(link)
                await db.commit()
                logger.info(
                    f"Added {user.email} user to group {default_group.id=}."
                )

            this_user = await db.get(UserOAuth, user.id)

            this_user.settings = UserSettings()
            await db.merge(this_user)
            await db.commit()
            await db.refresh(this_user)
            logger.info(
                f"Associated empty settings (id={this_user.user_settings_id}) "
                f"to '{this_user.email}'."
            )

            # Send mail section
            settings = Inject(get_settings)

            if (
                this_user.oauth_accounts
                and settings.email_settings is not None
            ):
                try:
                    logger.info(
                        "START sending email about new signup to "
                        f"{settings.email_settings.recipients}."
                    )
                    mail_new_oauth_signup(
                        msg=f"New user registered: '{this_user.email}'.",
                        email_settings=settings.email_settings,
                    )
                    logger.info("END sending email about new signup.")
                except Exception as e:
                    logger.error(
                        "ERROR sending notification email after oauth "
                        f"registration of {this_user.email}. "
                        f"Original error: '{e}'."
                    )


async def get_user_manager(
    user_db: SQLModelUserDatabaseAsync = Depends(get_user_db),
) -> AsyncGenerator[UserManager, None]:
    yield UserManager(user_db)


get_async_session_context = contextlib.asynccontextmanager(get_async_db)
get_user_db_context = contextlib.asynccontextmanager(get_user_db)
get_user_manager_context = contextlib.asynccontextmanager(get_user_manager)


async def _create_first_user(
    email: str,
    password: str,
    is_superuser: bool = False,
    is_verified: bool = False,
    username: str | None = None,
) -> None:
    """
    Private method to create the first fractal-server user

    Create a user with the given default arguments and return a message with
    the relevant information. If the user already exists, for example after a
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
        is_verified: `True` if the new user is verified
        username:
    """
    function_logger = set_logger("fractal_server.create_first_user")
    function_logger.info(f"START _create_first_user, with email '{email}'")
    try:
        async with get_async_session_context() as session:
            if is_superuser is True:
                # If a superuser already exists, exit
                stm = select(UserOAuth).where(  # noqa
                    UserOAuth.is_superuser == True  # noqa
                )  # noqa
                res = await session.execute(stm)
                existing_superuser = res.scalars().first()
                if existing_superuser is not None:
                    function_logger.info(
                        f"'{existing_superuser.email}' superuser already "
                        f"exists, skip creation of '{email}'"
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
                    function_logger.info(f"User '{user.email}' created")
    except UserAlreadyExists:
        function_logger.warning(f"User '{email}' already exists")
    except Exception as e:
        function_logger.error(
            f"ERROR in _create_first_user, original error {str(e)}"
        )
        raise e
    finally:
        function_logger.info(f"END   _create_first_user, with email '{email}'")


def _create_first_group():
    """
    Create a `UserGroup` with `name=FRACTAL_DEFAULT_GROUP_NAME`, if missing.
    """
    function_logger = set_logger("fractal_server.create_first_group")

    function_logger.info(
        f"START _create_first_group, with name '{FRACTAL_DEFAULT_GROUP_NAME}'"
    )
    with next(get_sync_db()) as db:
        group_all = db.execute(
            select(UserGroup).where(
                UserGroup.name == FRACTAL_DEFAULT_GROUP_NAME
            )
        )
        if group_all.scalars().one_or_none() is None:
            first_group = UserGroup(name=FRACTAL_DEFAULT_GROUP_NAME)
            db.add(first_group)
            db.commit()
            function_logger.info(
                f"Created group '{FRACTAL_DEFAULT_GROUP_NAME}'"
            )
        else:
            function_logger.info(
                f"Group '{FRACTAL_DEFAULT_GROUP_NAME}' already exists, skip."
            )
    function_logger.info(
        f"END   _create_first_group, with name '{FRACTAL_DEFAULT_GROUP_NAME}'"
    )
