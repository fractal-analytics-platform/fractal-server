import pytest

from fractal_server.app.db import DB


async def test_attribute_errore():

    with pytest.raises(AttributeError):
        DB._engine_async
    async_db = DB.get_async_db
    async for db in async_db():
        assert db
    DB._engine_async

    with pytest.raises(AttributeError):
        DB._engine_sync
    sync_db = DB.get_sync_db
    for db in sync_db():
        assert db
    DB._engine_sync
