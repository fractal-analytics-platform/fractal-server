import pytest
from devtools import debug

from fractal_server.app.db import DB
from tests.fixtures_server import DB_ENGINE


async def test_db_connection(db):
    # test active connection
    assert db.is_active
    # test bound
    assert db.get_bind()
    debug(db.get_bind())
    debug(db.get_bind().url.database)
    assert db.get_bind().url.database is not None

    from sqlmodel import select
    from fractal_server.app.models.security import UserOAuth

    stm = select(UserOAuth)
    res = await db.execute(stm)
    debug(res)


async def test_sync_db(db_sync, db):
    """
    GIVEN a database and a sync and an async connections to it
    WHEN crud operations are executed with one connection
    THEN results are consistent with the other connection
    """
    assert db_sync.is_active
    assert db_sync.get_bind()

    from sqlmodel import select
    from fractal_server.app.models.task import Task

    db.add(
        Task(
            name="mytask",
            input_type="image",
            output_type="zarr",
            command="cmd",
            source="/source",
        )
    )
    await db.commit()

    # Async
    stm = select(Task)
    res = await db.execute(stm)
    task_list = res.scalars().all()
    assert len(task_list) == 1
    assert task_list[0].name == "mytask"

    # Sync
    res = db_sync.execute(stm)
    task_list = res.scalars().all()
    assert len(task_list) == 1
    assert task_list[0].name == "mytask"


@pytest.mark.skipif(
    DB_ENGINE == "sqlite", reason="Skip if DB is SQLite, pass if it's Postgres"
)
def test_DB_ENGINE_is_postgres():
    pass


async def test_DB_class_async():

    with pytest.raises(AttributeError):
        DB._engine_async
    with pytest.raises(AttributeError):
        DB._async_session_maker
    DB.engine_async()
    DB._engine_async
    DB._async_session_maker
    delattr(DB, "_engine_async")
    delattr(DB, "_async_session_maker")
    with pytest.raises(AttributeError):
        DB._engine_async
    with pytest.raises(AttributeError):
        DB._async_session_maker
    await anext(DB.get_async_db())
    DB._engine_async
    DB._async_session_maker


def test_DB_class_sync():

    with pytest.raises(AttributeError):
        DB._engine_sync
    with pytest.raises(AttributeError):
        DB._sync_session_maker
    DB.engine_sync()
    DB._engine_sync
    DB._sync_session_maker
    delattr(DB, "_engine_sync")
    delattr(DB, "_sync_session_maker")
    with pytest.raises(AttributeError):
        DB._engine_sync
    with pytest.raises(AttributeError):
        DB._sync_session_maker
    next(DB.get_sync_db())
    DB._engine_sync
    DB._sync_session_maker
