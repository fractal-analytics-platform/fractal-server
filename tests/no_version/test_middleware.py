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
    Verifify that the function `_endpoint_has_background_task` works as
    expected.

    The test fails on the first assertion if there is an endpoint using
    `BackgroundTasks` for which the function returns `False`, e.g.:
    ```
    @router.get("/foo/bar/")
    async def example(background_tasks: BackgroundTasks):
        return {}
    ```

    The test fails on the second assertion if there is an endpoint for which
    the function returns `True`, but which is not listed below, e.g.:
    ```
    @router.post("/foo/task-group/bar/")
    async def example():
        return {}
    ```
    """
    background_task_routes = set()
    for route in app.routes:
        if isinstance(route, APIRoute):
            method = list(route.methods)[0]
            path = route.path
            has_background_task = False
            signature = inspect.signature(route.endpoint)
            for _, param in signature.parameters.items():
                if param.annotation == BackgroundTasks:
                    background_task_routes.add((method, path))
                    has_background_task = True
                    break

            assert (
                _endpoint_has_background_task(method=method, path=path)
                == has_background_task
            )
    assert background_task_routes == {
        ("POST", "/api/v2/project/{project_id}/job/submit/"),
        ("POST", "/api/v2/task/collect/pip/"),
        ("POST", "/api/v2/task/collect/pixi/"),
        ("POST", "/api/v2/task-group/{task_group_id}/deactivate/"),
        ("POST", "/api/v2/task-group/{task_group_id}/reactivate/"),
        ("POST", "/api/v2/task-group/{task_group_id}/delete/"),
        ("POST", "/admin/v2/task-group/{task_group_id}/deactivate/"),
        ("POST", "/admin/v2/task-group/{task_group_id}/reactivate/"),
        ("POST", "/admin/v2/task-group/{task_group_id}/delete/"),
    }
