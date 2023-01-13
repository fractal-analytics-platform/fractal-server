# Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
# University of Zurich
#
# Original authors:
# Jacopo Nespolo <jacopo.nespolo@exact-lab.it>
# Marco Franzon <marco.franzon@exact-lab.it>
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

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi_users.exceptions import UserAlreadyExists

from .app.db import get_db
from .app.security import get_user_db
from .app.security import get_user_manager
from .app.security import UserCreate
from .config import get_settings
from .syringe import Inject

get_async_session_context = contextlib.asynccontextmanager(get_db)
get_user_db_context = contextlib.asynccontextmanager(get_user_db)
get_user_manager_context = contextlib.asynccontextmanager(get_user_manager)


def collect_routers(app: FastAPI) -> None:
    """
    Register the routers to the application

    Args:
        app:
            The application to register the routers to.
    """
    from .app.api import router_default
    from .app.api import router_v1
    from .app.security import auth_router

    app.include_router(router_default, prefix="/api")
    app.include_router(router_v1, prefix="/api/v1")
    app.include_router(auth_router, prefix="/auth", tags=["auth"])


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


async def _create_user(
    email: str, password: str, slurm_user: str, is_superuser: bool = False
):
    """
    Private method for default fractal super-user at start-up.

    It creates a default users with default arguments and return
    a message with the relevant informations. If the user alredy exists,
    for example after a restart, it returns a message
    to inform that user already exists.
    """
    try:
        async with get_async_session_context() as session:
            async with get_user_db_context(session) as user_db:
                async with get_user_manager_context(user_db) as user_manager:
                    user = await user_manager.create(
                        UserCreate(
                            email=email,
                            password=password,
                            slurm_user=slurm_user,
                            is_superuser=is_superuser,
                        )
                    )
                    print("|--------------------------------|")
                    print(
                        f"| User created:                  |\n"
                        f"|   email: {user.email}      |\n"
                        f"|   password: 1234               |\n"
                        f"|   slurm_user: test             |"
                    )
                    print("|--------------------------------|")

    except UserAlreadyExists:
        print("Default user already exists")


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

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://127.0.0.1:5173", "http://localhost:5173"],
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
    await _create_user("admin@fractal.xy", "1234", "slurm", True)
    await __on_startup()
