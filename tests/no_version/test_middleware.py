import logging
import time

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

    async with AsyncClient(
        base_url="http://test", transport=ASGITransport(app=app)
    ) as client:
        caplog.clear()
        await client.get("/")
        assert caplog.text == ""

        caplog.clear()
        await client.get("/?sleep=0.1")
        assert "0.10 seconds" in caplog.text

        caplog.clear()
        await client.get("/?sleep=0.3")
        assert "0.30 seconds" in caplog.text
