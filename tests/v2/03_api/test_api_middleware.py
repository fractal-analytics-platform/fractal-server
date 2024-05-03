"""
See https://github.com/fractal-analytics-platform/fractal-server/issues/1406
"""
from typing import Any
from typing import AsyncGenerator

import pytest
from asgi_lifespan import LifespanManager
from devtools import debug
from fastapi import FastAPI
from httpx import AsyncClient

from fractal_server.app.routes.api import router_api


@router_api.get("/raise/")
async def internal_server_error():
    raise Exception("BUG!")


@pytest.fixture
async def client_error(
    app: FastAPI,
) -> AsyncGenerator[AsyncClient, Any]:
    app.include_router(router_api)
    async with AsyncClient(
        app=app, base_url="http://"
    ) as client, LifespanManager(app):
        yield client


async def test_internal_server_error(client_error):
    res = await client_error.get("/raise/")
    debug(res.json())
    assert res.status_code == 500
    assert res.json()["detail"] == "Internal server error occurred"
    assert res.json()["original_error"] == "BUG!"
