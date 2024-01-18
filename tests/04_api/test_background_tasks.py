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

from fractal_server.app.db import DBSyncSession
from fractal_server.app.db import get_db
from fractal_server.app.db import get_sync_db
from fractal_server.app.models import State
from fractal_server.app.routes.api import router_api
from fractal_server.logger import set_logger

logger = set_logger(__name__)


# BackgroundTasks functions


async def bgtask_sync_db(state_id: int):
    """
    This is a function to be executed as a background task, and it uses a
    sync db session.
    """
    new_db: DBSyncSession = next(get_sync_db())
    state = new_db.get(State, state_id)
    state.data = {"c": "d"}
    new_db.merge(state)
    new_db.commit()
    new_db.close()


async def bgtask_async_db(state_id: int):
    """
    This is a function to be executed as a background task, and it uses an
    async db session.
    """
    logger.critical("bgtask_async_db START")
    async for new_db in get_db():
        state = await new_db.get(State, state_id)
        state.data = {"a": "b"}
        await new_db.merge(state)
        await new_db.commit()
        await new_db.close()
    logger.critical("bgtask_async_db END")


# New endpoints and client


@router_api.get("/test_async/")
async def run_background_task_async(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    """Endpoint that calls bgtask_async_db in background."""
    logger.critical("START run_background_task_async")
    state = State()
    db.add(state)
    await db.commit()
    debug(state)
    state_id = state.id
    await db.close()
    logger.critical("END   run_background_task_async")

    background_tasks.add_task(bgtask_async_db, state_id)


@router_api.get("/test_sync/")
async def run_background_task_sync(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    """Endpoint that calls bgtask_sync_db in background."""

    logger.critical("START run_background_task_sync")
    state = State()
    db.add(state)
    await db.commit()
    debug(state)
    state_id = state.id
    await db.close()
    logger.critical("END   run_background_task_sync")

    background_tasks.add_task(bgtask_sync_db, state_id)


@pytest.fixture
async def client_for_bgtasks(
    app: FastAPI,
    db: AsyncSession,
) -> AsyncGenerator[AsyncClient, Any]:
    """Client wich includes the two new endpoints."""

    app.include_router(router_api, prefix="/test_bgtasks")
    async with AsyncClient(
        app=app, base_url="http://"
    ) as client, LifespanManager(app):
        yield client


async def test_async_db(db, client_for_bgtasks):
    """Call the run_background_task_async endpoint"""
    res = await client_for_bgtasks.get("/test_bgtasks/test_async/")
    debug(res)
    assert res.status_code == 200


async def test_sync_db(db, db_sync, client_for_bgtasks):
    """Call the run_background_task_sync endpoint"""
    res = await client_for_bgtasks.get("/test_bgtasks/test_sync/")
    debug(res)
    assert res.status_code == 200
