from fastapi import FastAPI
from sqlmodel import select

from fractal_server.app.models.security import UserOAuth
from fractal_server.main import lifespan


async def test_app_with_lifespan(caplog, db):

    app = FastAPI()
    res = await db.execute(select(UserOAuth))
    assert res.unique().all() == []

    async with lifespan(app):
        res = await db.execute(select(UserOAuth))
        assert len(res.unique().all()) == 1
