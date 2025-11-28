import pytest
from devtools import debug
from sqlmodel import delete
from sqlmodel import select

from fractal_server.app.db import DB
from fractal_server.app.models.security import UserOAuth


async def test_db_connection(db):
    # test active connection
    assert db.is_active
    # test bound
    assert db.get_bind()
    debug(db.get_bind())
    debug(db.get_bind().url.database)
    assert db.get_bind().url.database is not None

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

    db.add(
        UserOAuth(
            email="user@oauth.com",
            hashed_password="xxx",
            project_dirs=["/fake"],
        )
    )
    await db.commit()

    # Async
    stm = select(UserOAuth)
    res = await db.execute(stm)
    task_list = res.scalars().unique().all()
    assert len(task_list) == 1
    assert task_list[0].email == "user@oauth.com"

    # Sync
    res = db_sync.execute(stm)
    task_list = res.scalars().unique().all()
    assert len(task_list) == 1
    assert task_list[0].email == "user@oauth.com"


async def test_DB_class_async():
    try:
        assert DB._engine_async
        delattr(DB, "_engine_async")
    except AttributeError:
        pass
    with pytest.raises(AttributeError):
        assert DB._engine_async

    DB.engine_async()
    assert DB._engine_async
    delattr(DB, "_engine_async")

    try:
        assert DB._async_session_maker
        delattr(DB, "_async_session_maker")
    except AttributeError:
        pass
    with pytest.raises(AttributeError):
        assert DB._async_session_maker

    async for _ in DB.get_async_db():
        pass

    assert DB._engine_async
    assert DB._async_session_maker


def test_DB_class_sync():
    try:
        assert DB._engine_sync
        delattr(DB, "_engine_sync")
    except AttributeError:
        pass
    with pytest.raises(AttributeError):
        assert DB._engine_sync

    DB.engine_sync()
    assert DB._engine_sync
    delattr(DB, "_engine_sync")

    try:
        assert DB._sync_session_maker
        delattr(DB, "_sync_session_maker")
    except AttributeError:
        pass
    with pytest.raises(AttributeError):
        assert DB._sync_session_maker

    next(DB.get_sync_db())

    assert DB._engine_sync
    assert DB._sync_session_maker


async def test_reusing_id(db):
    """
    Tests different database behaviors with incremental IDs.

    https://github.com/fractal-analytics-platform/fractal-server/issues/1991
    """

    num_users = 10

    # Create `num_users` new users
    user_list = [
        UserOAuth(
            email=f"{x}@y.z",
            hashed_password="xxx",
            project_dirs=["/fake"],
        )
        for x in range(num_users)
    ]

    for user in user_list:
        db.add(user)
    await db.commit()
    for user in user_list:
        await db.refresh(user)

    # Extract list of IDs
    user_id_list = [user.id for user in user_list]

    # Remove all users
    await db.execute(delete(UserOAuth))
    res = await db.execute(select(UserOAuth))
    users = res.scalars().unique().all()
    assert len(users) == 0

    # Create `num_users + 10` new users
    new_user_list = [
        UserOAuth(
            email=f"{x}@y.z",
            hashed_password="xxx",
            project_dirs=["/fake"],
        )
        for x in range(num_users + 10)
    ]
    for user in new_user_list:
        db.add(user)
    await db.commit()
    for user in new_user_list:
        await db.refresh(user)

    # Extract list of IDs
    new_user_id_list = [user.id for user in new_user_list]

    # Assert IDs lists are disjoined
    assert set(new_user_id_list).isdisjoint(set(user_id_list))
