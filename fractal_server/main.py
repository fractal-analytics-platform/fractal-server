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
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .app.routes.aux._runner import _backend_supports_shutdown  # FIXME: change
from .app.runner.shutdown import cleanup_after_shutdown
from .app.security import _create_first_user
from .config import get_settings
from .logger import config_uvicorn_loggers
from .logger import get_logger
from .logger import reset_logger_handlers
from .logger import set_logger
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
    from .app.routes.api.v2 import router_api_v2
    from .app.routes.admin.v1 import router_admin_v1
    from .app.routes.admin.v2 import router_admin_v2
    from .app.routes.auth import router_auth

    settings = Inject(get_settings)

    app.include_router(router_api, prefix="/api")
    if settings.FRACTAL_API_V1_MODE == "include":
        app.include_router(router_api_v1, prefix="/api/v1")
        app.include_router(
            router_admin_v1, prefix="/admin/v1", tags=["V1 Admin area"]
        )
    app.include_router(router_api_v2, prefix="/api/v2")
    app.include_router(
        router_admin_v2, prefix="/admin/v2", tags=["V2 Admin area"]
    )
    app.include_router(router_auth, prefix="/auth", tags=["Authentication"])


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

    logger = set_logger("fractal_server_settings")
    logger.debug("Fractal Settings:")
    for key, value in settings.dict().items():
        if any(s in key.upper() for s in ["PASSWORD", "SECRET"]):
            value = "*****"
        logger.debug(f"  {key}: {value}")
    reset_logger_handlers(logger)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.jobsV1 = []
    app.state.jobsV2 = []
    logger = set_logger("fractal_server.lifespan")
    logger.info("Start application startup")
    check_settings()
    settings = Inject(get_settings)
    await _create_first_user(
        email=settings.FRACTAL_DEFAULT_ADMIN_EMAIL,
        password=settings.FRACTAL_DEFAULT_ADMIN_PASSWORD,
        username=settings.FRACTAL_DEFAULT_ADMIN_USERNAME,
        is_superuser=True,
        is_verified=True,
    )

    if settings.FRACTAL_RUNNER_BACKEND == "slurm_ssh":
        from fractal_server.ssh._fabric import get_ssh_connection

        app.state.connection = get_ssh_connection()
        logger.info(
            f"Created SSH connection "
            f"({app.state.connection.is_connected=})."
        )
    else:
        app.state.connection = None

    config_uvicorn_loggers()
    logger.info("End application startup")
    reset_logger_handlers(logger)
    yield
    logger = get_logger("fractal_server.lifespan")
    logger.info("Start application shutdown")

    if settings.FRACTAL_RUNNER_BACKEND == "slurm_ssh":
        logger.info(
            f"Closing SSH connection "
            f"(current: {app.state.connection.is_connected=})."
        )

        app.state.connection.close()

    logger.info(
        f"Current worker with pid {os.getpid()} is shutting down. "
        f"Current jobs: {app.state.jobsV1=}, {app.state.jobsV2=}"
    )
    if _backend_supports_shutdown(settings.FRACTAL_RUNNER_BACKEND):
        try:
            await cleanup_after_shutdown(
                jobsV1=app.state.jobsV1,
                jobsV2=app.state.jobsV2,
                logger_name="fractal_server.lifespan",
            )
        except Exception as e:
            logger.error(
                "Something went wrong during shutdown phase, "
                "some of running jobs are not shutdown properly. "
                f"Original error: {e}"
            )
    else:
        logger.info("Shutdown not available for this backend runner.")

    logger.info("End application shutdown")
    reset_logger_handlers(logger)


def start_application() -> FastAPI:
    """
    Create the application, initialise it and collect all available routers.

    Returns:
        app:
            The fully initialised application.
    """
    app = FastAPI(lifespan=lifespan)
    collect_routers(app)
    return app


app = start_application()
