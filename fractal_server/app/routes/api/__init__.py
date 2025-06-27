"""
`api` module
"""
from fastapi import APIRouter
from fastapi import Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from ....config import get_settings
from ....syringe import Inject
from fractal_server.app.db import get_async_db
from fractal_server.app.db import get_sync_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth import current_active_superuser

router_api = APIRouter()


@router_api.get("/alive/")
async def alive():
    settings = Inject(get_settings)
    return dict(
        alive=True,
        version=settings.PROJECT_VERSION,
    )


@router_api.get("/settings/")
async def view_settings(user: UserOAuth = Depends(current_active_superuser)):
    settings = Inject(get_settings)
    return settings.model_dump()


@router_api.get("/db-async-dependency/")
async def test_db_async(
    db: AsyncSession = Depends(get_async_db),
):
    res = await db.execute(select(UserOAuth.id))
    res.scalars().all()
    return "OK"


@router_api.get("/db-sync-dependency/")
async def test_db_sync(
    db: Session = Depends(get_sync_db),
):
    res = db.execute(select(UserOAuth.id))
    res.scalars().all()
    return "OK"


@router_api.get("/db-async-context/")
async def test_db_async_context():
    async for db in get_async_db():
        res = await db.execute(select(UserOAuth.id))
        res.scalars().all()
    return "OK"


@router_api.get("/db-sync-context/")
async def test_db_sync_context(
    db: Session = Depends(get_sync_db),
):
    with next(get_sync_db()) as db:
        res = db.execute(select(UserOAuth.id))
        res.scalars().all()
    return "OK"
