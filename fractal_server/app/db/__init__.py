"""
`db` module, loosely adapted from
https://testdriven.io/blog/fastapi-sqlmodel/#async-sqlmodel
"""
from collections.abc import AsyncGenerator
from collections.abc import Generator

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import Session as DBSyncSession
from sqlalchemy.orm import sessionmaker

from ...config import get_settings
from ...logger import set_logger
from ...syringe import Inject


logger = set_logger(__name__)


class DB:
    """
    DB class
    """

    @classmethod
    def engine_async(cls):
        try:
            return cls._engine_async
        except AttributeError:
            cls.set_async_db()
            return cls._engine_async

    @classmethod
    def engine_sync(cls):
        try:
            return cls._engine_sync
        except AttributeError:
            cls.set_sync_db()
            return cls._engine_sync

    @classmethod
    def set_async_db(cls):
        settings = Inject(get_settings)
        settings.check_db()

        cls._engine_async = create_async_engine(
            settings.DATABASE_ASYNC_URL,
            echo=settings.DB_ECHO,
            future=True,
            pool_pre_ping=True,
        )
        cls._async_session_maker = sessionmaker(
            cls._engine_async,
            class_=AsyncSession,
            expire_on_commit=False,
            future=True,
        )

    @classmethod
    def set_sync_db(cls):
        settings = Inject(get_settings)
        settings.check_db()

        cls._engine_sync = create_engine(
            settings.DATABASE_SYNC_URL,
            echo=settings.DB_ECHO,
            future=True,
            pool_pre_ping=True,
        )

        cls._sync_session_maker = sessionmaker(
            bind=cls._engine_sync,
            autocommit=False,
            autoflush=False,
            future=True,
        )

    @classmethod
    async def get_async_db(cls) -> AsyncGenerator[AsyncSession, None]:
        """
        Get async database session
        """
        try:
            session_maker = cls._async_session_maker()
        except AttributeError:
            cls.set_async_db()
            session_maker = cls._async_session_maker()
        async with session_maker as async_session:
            yield async_session

    @classmethod
    def get_sync_db(cls) -> Generator[DBSyncSession, None, None]:
        """
        Get sync database session
        """
        try:
            session_maker = cls._sync_session_maker()
        except AttributeError:
            cls.set_sync_db()
            session_maker = cls._sync_session_maker()
        with session_maker as sync_session:
            yield sync_session


get_async_db = DB.get_async_db
get_sync_db = DB.get_sync_db
