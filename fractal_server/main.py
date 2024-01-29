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
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .app.security import _create_first_user
from .config import get_settings
from .syringe import Inject


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

    Verify the consistency of the settings, in particular that required
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
