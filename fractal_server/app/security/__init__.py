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
from typing import Self

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
from fractal_server.app.schemas.user import UserCreate
from fractal_server.app.security.signup_email import (
    send_fractal_email_or_log_failure,
)
from fractal_server.config import get_email_settings
from fractal_server.config import get_settings
from fractal_server.logger import close_logger
from fractal_server.logger import set_logger
from fractal_server.syringe import Inject

logger = set_logger(__name__)


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

    async def oauth_callback(
        self: Self,
        oauth_name: str,
        access_token: str,
        account_id: str,
        account_email: str,
        expires_at: int | None = None,
        refresh_token: str | None = None,
        request: Request | None = None,
        *,
        associate_by_email: bool = False,
        is_verified_by_default: bool = False,
    ) -> UserOAuth:
        """
        Handle the callback after a successful OAuth authentication.

        This method extends the corresponding `BaseUserManager` method of
        > fastapi-users v14.0.1, Copyright (c) 2019 François Voron, MIT License

        If the user already exists with this OAuth account, the token is
        updated.

        If a user with the same e-mail already exists and `associate_by_email`
        is True, the OAuth account is associated to this user.
        Otherwise, the `UserNotExists` exception is raised.

        If the user does not exist, send an email to the Fractal admins (if
        configured) and respond with a 400 error status. NOTE: This is the
        function branch where the `fractal-server` implementation deviates
        from the original `fastapi-users` one.

        :param oauth_name: Name of the OAuth client.
        :param access_token: Valid access token for the service provider.
        :param account_id: models.ID of the user on the service provider.
        :param account_email: E-mail of the user on the service provider.
        :param expires_at: Optional timestamp at which the access token
        expires.
        :param refresh_token: Optional refresh token to get a
        fresh access token from the service provider.
        :param request: Optional FastAPI request that
        triggered the operation, defaults to None
        :param associate_by_email: If True, any existing user with the same
        e-mail address will be associated to this user. Defaults to False.
        :param is_verified_by_default: If True, the `is_verified` flag will be
        set to `True` on newly created user. Make sure the OAuth Provider you
        are using does verify the email address before enabling this flag.
        Defaults to False.
        :return: A user.
        """
        from fastapi import HTTPException
        from fastapi import status
        from fastapi_users import exceptions

        oauth_account_dict = {
            "oauth_name": oauth_name,
            "access_token": access_token,
            "account_id": account_id,
            "account_email": account_email,
            "expires_at": expires_at,
            "refresh_token": refresh_token,
        }

        try:
            user = await self.get_by_oauth_account(oauth_name, account_id)
        except exceptions.UserNotExists:
            try:
                # Associate account
                user = await self.get_by_email(account_email)
                if not associate_by_email:
                    raise exceptions.UserAlreadyExists()
                user = await self.user_db.add_oauth_account(
                    user, oauth_account_dict
                )
            except exceptions.UserNotExists:
                # (0) Log
                logger.warning(
                    f"Self-registration attempt by {account_email}."
                )

                # (1) Prepare user-facing error message
                error_msg = (
                    "Thank you for registering for the Fractal service. "
                    "Administrators have been informed to configure your "
                    "account and will get back to you."
                )
                settings = Inject(get_settings)
                if settings.FRACTAL_HELP_URL is not None:
                    error_msg = (
                        f"{error_msg}\n"
                        "You can find more information about the onboarding "
                        f"process at {settings.FRACTAL_HELP_URL}."
                    )

                # (2) Send email to admins
                email_settings = Inject(get_email_settings)
                send_fractal_email_or_log_failure(
                    subject="New OAuth self-registration",
                    msg=(
                        f"User '{account_email}' tried to "
                        "self-register through OAuth.\n"
                        "Please create the Fractal account manually.\n"
                        "Here is the error message displayed to the "
                        f"user:\n{error_msg}"
                    ),
                    email_settings=email_settings.public,
                )

                # (3) Raise
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=error_msg,
                )
        else:
            # Update oauth
            for existing_oauth_account in user.oauth_accounts:
                if (
                    existing_oauth_account.account_id == account_id
                    and existing_oauth_account.oauth_name == oauth_name
                ):
                    user = await self.user_db.update_oauth_account(
                        user, existing_oauth_account, oauth_account_dict
                    )

        return user

    async def on_after_register(
        self, user: UserOAuth, request: Request | None = None
    ):
        settings = Inject(get_settings)
        logger.info(
            f"New-user registration completed ({user.id=}, {user.email=})."
        )
        async for db in get_async_db():
            # Note: if `FRACTAL_DEFAULT_GROUP_NAME=None`, this query will
            # result into `None`
            settings = Inject(get_settings)
            stm = select(UserGroup.id).where(
                UserGroup.name == settings.FRACTAL_DEFAULT_GROUP_NAME
            )
            res = await db.execute(stm)
            default_group_id_or_none = res.scalars().one_or_none()
            if default_group_id_or_none is not None:
                link = LinkUserGroup(
                    user_id=user.id, group_id=default_group_id_or_none
                )
                db.add(link)
                await db.commit()
                logger.info(
                    f"Added {user.email} user to group "
                    f"{default_group_id_or_none=}."
                )
            elif settings.FRACTAL_DEFAULT_GROUP_NAME is not None:
                logger.error(
                    "No group found with name "
                    f"{settings.FRACTAL_DEFAULT_GROUP_NAME}"
                )
            # NOTE: the `else` of this branch would simply be a `pass`. The
            # "All" group was not found, but this is not worth a WARNING
            # because `FRACTAL_DEFAULT_GROUP_NAME` is set to `None` in the
            # settings.


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
    project_dir: str,
    profile_id: int | None = None,
    is_superuser: bool = False,
    is_verified: bool = False,
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

    Args:
        email: New user's email
        password: New user's password
        is_superuser: `True` if the new user is a superuser
        is_verified: `True` if the new user is verified
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
                        project_dir=project_dir,
                        profile_id=profile_id,
                        is_superuser=is_superuser,
                        is_verified=is_verified,
                    )
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
        close_logger(function_logger)


def _create_first_group():
    """
    Create a `UserGroup` named `FRACTAL_DEFAULT_GROUP_NAME`, if this variable
    is set and if such a group does not already exist.
    """
    settings = Inject(get_settings)
    function_logger = set_logger("fractal_server.create_first_group")

    if settings.FRACTAL_DEFAULT_GROUP_NAME is None:
        function_logger.info(
            f"SKIP because '{settings.FRACTAL_DEFAULT_GROUP_NAME=}'"
        )
        return

    function_logger.info(
        f"START, name '{settings.FRACTAL_DEFAULT_GROUP_NAME}'"
    )
    with next(get_sync_db()) as db:
        group_all = db.execute(
            select(UserGroup).where(
                UserGroup.name == settings.FRACTAL_DEFAULT_GROUP_NAME
            )
        )
        if group_all.scalars().one_or_none() is None:
            first_group = UserGroup(name=settings.FRACTAL_DEFAULT_GROUP_NAME)
            db.add(first_group)
            db.commit()
            function_logger.info(
                f"Created group '{settings.FRACTAL_DEFAULT_GROUP_NAME}'"
            )
        else:
            function_logger.info(
                f"Group '{settings.FRACTAL_DEFAULT_GROUP_NAME}' "
                "already exists, skip."
            )
    function_logger.info("END")
    close_logger(function_logger)
