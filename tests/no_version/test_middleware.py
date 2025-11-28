import logging
import time

from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from httpx import ASGITransport
from httpx import AsyncClient

from fractal_server.main import SlowResponseMiddleware


async def test_app_with_middleware(caplog):
    app = FastAPI()
    app.add_middleware(SlowResponseMiddleware, time_threshold=0.01)

    @app.get("/")
    async def root(sleep: float):
        time.sleep(sleep)
        return {"message": "Hello World"}

    logger = logging.getLogger("slow-response")
    logger.propagate = True

    async with (
        AsyncClient(
            base_url="http://test", transport=ASGITransport(app=app)
        ) as client,
        LifespanManager(app),
    ):
        caplog.clear()
        await client.get("/")
        assert caplog.text == ""

        caplog.clear()
        await client.get("/?sleep=0.12")
        assert "0.12 seconds" in caplog.text

        caplog.clear()
        await client.get("/?sleep=0.33&foo=bar")
        assert "0.33 seconds" in caplog.text
        assert "GET /?sleep=0.33&foo=bar" in caplog.text
