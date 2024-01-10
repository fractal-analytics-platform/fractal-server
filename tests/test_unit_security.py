from sqlmodel import select

from fractal_server.app.models.security import UserOAuth
from fractal_server.app.security import _create_first_user


async def count_users(db):
    res = await db.execute(select(UserOAuth))
    return len(res.unique().scalars().all())


async def test_unit_create_first_user(db):

    assert await count_users(db) == 0

    await _create_first_user(email="test1@fractal.com", password="xxxx")
    assert await count_users(db) == 1

    await _create_first_user(email="test2@fractal.com", password="xxxx")
    assert await count_users(db) == 2
