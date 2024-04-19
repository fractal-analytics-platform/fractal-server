import logging

from sqlmodel import select

from fractal_server.app.models.security import UserOAuth
from fractal_server.app.security import _create_first_user


async def count_users(db):
    res = await db.execute(select(UserOAuth))
    return len(res.unique().scalars().all())


async def test_unit_create_first_user(db, caplog):

    assert await count_users(db) == 0

    # Calls that do create a new user

    await _create_first_user(email="test1@fractal.com", password="xxxx")
    assert await count_users(db) == 1

    await _create_first_user(
        email="test2@fractal.com", password="xxxx", username="test2"
    )
    assert await count_users(db) == 2

    await _create_first_user(
        email="test3@fractal.com", password="xxxx", is_superuser=True
    )
    assert await count_users(db) == 3

    await _create_first_user(
        email="test4@fractal.com", password="xxxx", is_verified=True
    )
    assert await count_users(db) == 4

    # Calls that do not create new users

    # Cannot create two users with the same email
    with caplog.at_level(logging.WARNING):
        await _create_first_user(email="test2@fractal.com", password="xxxx")
    assert "User test2@fractal.com already exists" in caplog.text
    assert await count_users(db) == 4
    caplog.clear()

    # Cannot create more than one superuser
    with caplog.at_level(logging.INFO):
        await _create_first_user(
            email="test5@fractal.com", password="xxxx", is_superuser=True
        )
    assert "superuser already exists, skip creation" in caplog.text
    assert await count_users(db) == 4
    caplog.clear()
