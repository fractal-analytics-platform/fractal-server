import os
import time
from contextlib import asynccontextmanager
from datetime import datetime
from itertools import chain

from fastapi import FastAPI
from starlette.types import Message
from starlette.types import Receive
from starlette.types import Scope
from starlette.types import Send

from fractal_server import __VERSION__
from fractal_server.app.schemas.v2 import ResourceType

from .app.routes.aux._runner import _backend_supports_shutdown
from .app.shutdown import cleanup_after_shutdown
from .config import get_db_settings
from .config import get_email_settings
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
    from .app.routes.admin.v2 import router_admin
    from .app.routes.api import router_api
    from .app.routes.api.v2 import router_api as router_api_v2
    from .app.routes.auth.router import router_auth

    app.include_router(router_api, prefix="/api")
    app.include_router(router_api_v2, prefix="/api/v2")
    app.include_router(router_admin, prefix="/admin/v2", tags=["Admin area"])
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
    db_settings = Inject(get_db_settings)
    email_settings = Inject(get_email_settings)
    logger = set_logger("fractal_server_settings")
    logger.debug("Fractal Settings:")
    for key, value in chain(
        db_settings.model_dump().items(),
        settings.model_dump().items(),
        email_settings.model_dump().items(),
    ):
        if any(s in key.upper() for s in ["PASSWORD", "SECRET", "KEY"]):
            value = "*****"
        logger.debug(f"  {key}: {value}")
    reset_logger_handlers(logger)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.jobs = []
    logger = set_logger("fractal_server.lifespan")
    logger.info(f"[startup] START (fractal-server {__VERSION__})")
    check_settings()
    settings = Inject(get_settings)

    if settings.FRACTAL_RUNNER_BACKEND == ResourceType.SLURM_SSH:
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

    if settings.FRACTAL_RUNNER_BACKEND == ResourceType.SLURM_SSH:
        logger.info(
            "[teardown] Close FractalSSH connections "
            f"(current size: {app.state.fractal_ssh_list.size})."
        )

        app.state.fractal_ssh_list.close_all()

    logger.info(
        f"[teardown] Current worker with pid {os.getpid()} is shutting down. "
        f"Current jobs: {app.state.jobs=}"
    )
    if _backend_supports_shutdown(settings.FRACTAL_RUNNER_BACKEND):
        try:
            await cleanup_after_shutdown(
                jobs=app.state.jobs,
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


slow_response_logger = set_logger("slow-response")


def _endpoint_has_background_task(method: str, path: str) -> bool:
    has_background_task = (method == "POST") and (
        "/job/submit/" in path
        or "/task/collect/pi" in path  # "/pip" and "/pixi"
        or "/task-group/" in path
    )
    return has_background_task


class SlowResponseMiddleware:
    def __init__(self, app: FastAPI, time_threshold: float):
        self.app = app
        self.time_threshold = time_threshold

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if (
            scope["type"] != "http"  # e.g. `scope["type"] == "lifespan"`
            or _endpoint_has_background_task(scope["method"], scope["path"])
        ):
            await self.app(scope, receive, send)
            return

        # Mutable variable which can be updated from within `send_wrapper`
        context = {"status_code": None}

        async def send_wrapper(message: Message):
            if message["type"] == "http.response.start":
                context["status_code"] = message["status"]
            await send(message)

        # Measure request time
        start_timestamp = datetime.now()
        start_time = time.perf_counter()
        await self.app(scope, receive, send_wrapper)
        stop_time = time.perf_counter()
        request_time = stop_time - start_time

        # Log if process time is too high
        if request_time > self.time_threshold:
            end_timestamp = datetime.now()
            slow_response_logger.warning(
                f"{scope['method']} {scope['route'].path}"
                f"?{scope['query_string'].decode('utf-8')}, "
                f"{context['status_code']}, "
                f"{request_time:.2f}, "
                f"{start_timestamp.isoformat(timespec='milliseconds')}, "
                f"{end_timestamp.isoformat(timespec='milliseconds')}"
            )


def start_application() -> FastAPI:
    """
    Create the application, initialise it and collect all available routers.

    Returns:
        app:
            The fully initialised application.
    """
    app = FastAPI(lifespan=lifespan)

    settings = Inject(get_settings)
    app.add_middleware(
        SlowResponseMiddleware,
        time_threshold=settings.FRACTAL_LONG_REQUEST_TIME,
    )

    collect_routers(app)
    return app


app = start_application()
