import inspect
import logging
import time

from asgi_lifespan import LifespanManager
from fastapi import BackgroundTasks
from fastapi import FastAPI
from fastapi.routing import APIRoute
from httpx import ASGITransport
from httpx import AsyncClient

from fractal_server.main import SlowResponseMiddleware
from fractal_server.main import _endpoint_has_background_task


async def test_app_with_middleware(caplog):
    app = FastAPI()
    app.add_middleware(SlowResponseMiddleware, time_threshold=0.01)

    @app.get("/")
    async def root(sleep: float):
        time.sleep(sleep)
        return {"message": "Hello World"}

    # Patch the logger that is used in the middleware, to capture it via caplog
    logger = logging.getLogger("slow-response")
    logger.propagate = True

    caplog.set_level(logging.WARNING)

    async with (
        AsyncClient(
            base_url="http://test",
            transport=ASGITransport(app=app),
        ) as client,
        LifespanManager(app),
    ):
        caplog.clear()
        await client.get("/?sleep=0")
        assert caplog.text == ""

        caplog.clear()
        await client.get("/?sleep=0.12")
        assert "0.12," in caplog.text

        caplog.clear()
        await client.get("/?sleep=0.33&foo=bar")
        assert "0.33," in caplog.text
        assert "GET /?sleep=0.33&foo=bar" in caplog.text


async def test_endpoint_has_background_task(app: FastAPI, register_routers):
    """
    Test that `_endpoint_has_background_task` correctly identifies endpoints
    containing a background task.
    """
    total = 0
    for route in app.routes:
        if isinstance(route, APIRoute):
            has_background_task = False
            signature = inspect.signature(route.endpoint)
            for _, param in signature.parameters.items():
                if param.annotation == BackgroundTasks:
                    has_background_task = True
                    total += 1
                    break

            assert (
                _endpoint_has_background_task(
                    method=list(route.methods)[0],
                    path=route.path,
                )
                == has_background_task
            )
    assert total == 9  # last edit 2.18.0
