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

from .app.routes.aux._runner import _backend_supports_shutdown
from .app.runner.shutdown import cleanup_after_shutdown
from .config import get_settings
from .logger import config_uvicorn_loggers
from .logger import get_logger
from .logger import reset_logger_handlers
from .logger import set_logger
from .syringe import Inject
from fractal_server import __VERSION__


def collect_routers(app: FastAPI) -> None:
    """
    Register the routers to the application

    Args:
        app:
            The application to register the routers to.
    """
    from .app.routes.api import router_api
    from .app.routes.api.v2 import router_api_v2
    from .app.routes.admin.v2 import router_admin_v2
    from .app.routes.auth.router import router_auth

    app.include_router(router_api, prefix="/api")
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
    for key, value in settings.model_dump().items():
        if any(s in key.upper() for s in ["PASSWORD", "SECRET", "KEY"]):
            value = "*****"
        logger.debug(f"  {key}: {value}")
    reset_logger_handlers(logger)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.jobsV2 = []
    logger = set_logger("fractal_server.lifespan")
    logger.info(f"[startup] START (fractal-server {__VERSION__})")
    check_settings()
    settings = Inject(get_settings)

    if settings.FRACTAL_RUNNER_BACKEND == "slurm_ssh":
        from fractal_server.ssh._fabric import FractalSSHList

        app.state.fractal_ssh_list = FractalSSHList()

        logger.info(
            "[startup] Added empty FractalSSHList to app.state "
            f"(id={id(app.state.fractal_ssh_list)})."
        )
    else:
        app.state.fractal_ssh_list = None

    config_uvicorn_loggers()
    logger.info("[startup] END")
    reset_logger_handlers(logger)

    yield

    logger = get_logger("fractal_server.lifespan")
    logger.info("[teardown] START")

    if settings.FRACTAL_RUNNER_BACKEND == "slurm_ssh":
        logger.info(
            "[teardown] Close FractalSSH connections "
            f"(current size: {app.state.fractal_ssh_list.size})."
        )

        app.state.fractal_ssh_list.close_all()

    logger.info(
        f"[teardown] Current worker with pid {os.getpid()} is shutting down. "
        f"Current jobs: {app.state.jobsV2=}"
    )
    if _backend_supports_shutdown(settings.FRACTAL_RUNNER_BACKEND):
        try:
            await cleanup_after_shutdown(
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
        logger.info(
            "[teardown] Shutdown not available for this backend runner."
        )

    logger.info("[teardown] END")
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
