# Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
# University of Zurich
#
# Original authors:
# Jacopo Nespolo <jacopo.nespolo@exact-lab.it>
# Marco Franzon <marco.franzon@exact-lab.it>
# Tommaso Comaprin <tommaso.comparin@exact-lab.it>
#
# This file is part of Fractal and was originally developed by eXact lab S.r.l.
# <exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
# Institute for Biomedical Research and Pelkmans Lab from the University of
# Zurich.
"""
# Application factory

This module sets up the FastAPI application that serves the Fractal Server.
"""
import contextlib
from typing import Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi_users.exceptions import UserAlreadyExists
from sqlalchemy.exc import IntegrityError
from sqlmodel import select

from .app.db import get_db
from .app.models.security import UserOAuth
from .app.schemas.user import UserCreate
from .app.security import get_user_db
from .app.security import get_user_manager
from .config import get_settings
from .logger import set_logger
from .syringe import Inject

get_async_session_context = contextlib.asynccontextmanager(get_db)
get_user_db_context = contextlib.asynccontextmanager(get_user_db)
get_user_manager_context = contextlib.asynccontextmanager(get_user_manager)

logger = set_logger(__name__)


def collect_routers(app: FastAPI) -> None:
    """
    Register the routers to the application

    Args:
        app:
            The application to register the routers to.
    """
    from .app.routes.api import router_api
    from .app.routes.api.v1 import router_api_v1
    from .app.routes.admin import router_admin
    from .app.routes.auth import router_auth

    app.include_router(router_api, prefix="/api")
    app.include_router(router_api_v1, prefix="/api/v1")
    app.include_router(router_admin, prefix="/admin", tags=["Admin area"])
    app.include_router(router_auth, prefix="/auth", tags=["auth"])


def check_settings() -> None:
    """
    Check and register the settings

    Verify the consistency of the settings, in particular that mandatory
    variables are set.

    Raises:
        ValidationError: If the configuration is invalid.
    """
    settings = Inject(get_settings)
    settings.check()


async def __on_startup() -> None:
    """
    Private wrapper for routines that need to be executed at server start-up.

    It should only be called from a `@app.on_event("startup")`-decorated
    callable.
    """
    check_settings()


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

            # If a superuser already exists, exit
            stm = select(UserOAuth).where(
                UserOAuth.is_superuser == True  # noqa: E712
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


def start_application() -> FastAPI:
    """
    Create and initialise the application

    It performs the following initialisation steps:

    1. Collect all available routers
    2. Set-up CORS middleware

    Returns:
        app:
            The fully initialised application.
    """
    app = FastAPI()
    collect_routers(app)
    settings = Inject(get_settings)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.FRACTAL_CORS_ALLOW_ORIGIN.split(";"),
        allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=[
            "set-cookie",
            "Set-Cookie",
            "Content-Type",
            "Access-Control-Allow-Headers",
            "X-Requested-With",
        ],
        allow_credentials=True,
    )

    return app


app = start_application()


@app.on_event("startup")
async def on_startup() -> None:
    """
    Register the starup calls

    If the calls raise any error, the application startup is aborted.
    """
    settings = Inject(get_settings)
    await _create_first_user(
        email=settings.FRACTAL_DEFAULT_ADMIN_EMAIL,
        password=settings.FRACTAL_DEFAULT_ADMIN_PASSWORD,
        username=settings.FRACTAL_DEFAULT_ADMIN_USERNAME,
        is_superuser=True,
        is_verified=True,
    )
    await __on_startup()
