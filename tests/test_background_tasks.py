"""
See https://github.com/fractal-analytics-platform/fractal-server/issues/661
"""
from typing import Any
from typing import AsyncGenerator

import pytest
from asgi_lifespan import LifespanManager
from devtools import debug
from fastapi import BackgroundTasks
from fastapi import Depends
from fastapi import FastAPI
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from fractal_server.app.api import router_default
from fractal_server.app.db import DBSyncSession
from fractal_server.app.db import get_db
from fractal_server.app.db import get_sync_db
from fractal_server.app.models import State
from fractal_server.logger import set_logger

logger = set_logger(__name__)


async def bgtask_async_db():
    new_db: AsyncSession = await get_db().__anext__()
    state = await new_db.get(State, 1)
    state.data = {"a": "b"}
    await new_db.merge(state)
    await new_db.commit()
    await new_db.close()


async def bgtask_sync_db():
    new_db: DBSyncSession = next(get_sync_db())
    state = new_db.get(State, 1)
    state.data = {"c": "d"}
    new_db.merge(state)
    new_db.commit()
    new_db.close()


@router_default.get("/test_async")
async def run_background_task_async(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):

    logger.critical("ENDPOINT START")
    state1 = State()
    db.add(state1)
    await db.commit()
    logger.critical("ENDPOINT END")

    background_tasks.add_task(bgtask_async_db)


@router_default.get("/test_sync")
async def run_background_task_sync(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):

    logger.critical("ENDPOINT START")
    state1 = State()
    db.add(state1)
    await db.commit()
    logger.critical("ENDPOINT END")

    background_tasks.add_task(bgtask_sync_db)


@pytest.fixture
async def client_for_bgtasks(
    app: FastAPI,
    db: AsyncSession,
) -> AsyncGenerator[AsyncClient, Any]:

    app.include_router(router_default, prefix="/api/bgtasks")
    async with AsyncClient(
        app=app, base_url="http://test"
    ) as client, LifespanManager(app):
        yield client


async def test_async_db(db, client_for_bgtasks):
    res = await client_for_bgtasks.get("http://test/api/bgtasks/test_async")
    debug(res)


async def test_sync_db(db, client_for_bgtasks):
    res = await client_for_bgtasks.get("http://test/api/bgtasks/test_sync")
    debug(res)
