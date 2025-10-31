import logging

from sqlmodel import select

import fractal_server.app.security
from fractal_server.app.models import UserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.security import _create_first_group
from fractal_server.app.security import _create_first_user


async def count_users(db):
    res = await db.execute(select(UserOAuth))
    return len(res.unique().scalars().all())


async def count_groups(db):
    res = await db.execute(select(UserGroup))
    return len(res.unique().scalars().all())


async def test_unit_create_first_user(db):
    assert await count_users(db) == 0

    # Calls that do create a new user

    await _create_first_user(
        email="test1@fractal.com",
        password="xxxx",
        project_dir="/fake",
    )
    assert await count_users(db) == 1

    await _create_first_user(
        email="test2@fractal.com",
        password="xxxx",
        project_dir="/fake",
    )
    assert await count_users(db) == 2

    await _create_first_user(
        email="test3@fractal.com",
        password="xxxx",
        is_superuser=True,
        project_dir="/fake",
    )
    assert await count_users(db) == 3

    await _create_first_user(
        email="test4@fractal.com",
        password="xxxx",
        is_verified=True,
        project_dir="/fake",
    )
    assert await count_users(db) == 4

    # Calls that do not create new users

    # Cannot create two users with the same email
    await _create_first_user(
        email="test2@fractal.com",
        password="xxxx",
        project_dir="/fake",
    )
    assert await count_users(db) == 4

    # Cannot create more than one superuser
    await _create_first_user(
        email="test5@fractal.com",
        password="xxxx",
        is_superuser=True,
        project_dir="/fake",
    )
    assert await count_users(db) == 4


async def test_unit_create_first_group(
    db, override_settings_factory, monkeypatch, caplog
):
    assert await count_groups(db) == 0
    # First call is effective
    _create_first_group()
    assert await count_groups(db) == 1
    # Second call is a no-op
    _create_first_group()
    assert await count_groups(db) == 1

    override_settings_factory(FRACTAL_DEFAULT_GROUP_NAME=None)
    LOGGER_NAME = "test"
    monkeypatch.setattr(
        fractal_server.app.security,
        "set_logger",
        lambda _: logging.getLogger(LOGGER_NAME),
    )
    with caplog.at_level(logging.INFO, logger=LOGGER_NAME):
        _create_first_group()
    assert (
        "SKIP because 'settings.FRACTAL_DEFAULT_GROUP_NAME=None'"
        in caplog.text
    )
