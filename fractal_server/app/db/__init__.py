"""
`db` module, loosely adapted from
https://testdriven.io/blog/fastapi-sqlmodel/#async-sqlmodel
"""
from typing import AsyncGenerator
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import Session as DBSyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from ...config import get_settings
from ...logger import set_logger
from ...syringe import Inject


print(__name__)
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
            cls.set_db()
            return cls._engine_async

    @classmethod
    def engine_sync(cls):
        try:
            return cls._engine_sync
        except AttributeError:
            cls.set_db()
            return cls._engine_sync

    @classmethod
    def set_db(cls):
        settings = Inject(get_settings)

        if settings.DB_ENGINE == "sqlite":
            logger.warning(
                "SQLite is supported but discouraged in production. Given its "
                "partial support for ForeignKey constraints, consistency of "
                "the database cannot be guaranteed."
            )

        # Set some sqlite-specific options
        if settings.DB_ENGINE == "sqlite":
            engine_kwargs_async = dict(poolclass=StaticPool)
            engine_kwargs_sync = dict(
                poolclass=StaticPool,
                connect_args={"check_same_thread": False},
            )
        else:
            engine_kwargs_async = {}
            engine_kwargs_sync = {}

        cls._engine_async = create_async_engine(
            settings.DATABASE_URL,
            echo=settings.DB_ECHO,
            future=True,
            **engine_kwargs_async,
        )
        cls._engine_sync = create_engine(
            settings.DATABASE_SYNC_URL,
            echo=settings.DB_ECHO,
            future=True,
            **engine_kwargs_sync,
        )

        cls._async_session_maker = sessionmaker(
            cls._engine_async, class_=AsyncSession, expire_on_commit=False
        )

        cls._sync_session_maker = sessionmaker(
            bind=cls._engine_sync, autocommit=False, autoflush=False
        )

    @classmethod
    async def get_db(cls) -> AsyncGenerator[AsyncSession, None]:
        """
        Get async database session
        """
        try:
            session_maker = cls._async_session_maker()
        except AttributeError:
            cls.set_db()
            session_maker = cls._async_session_maker()
        async with session_maker as async_session:
            yield async_session

    @classmethod
    def get_sync_db(cls) -> Generator[DBSyncSession, None, None]:
        """
        Get sync database session
        """
        with cls._sync_session_maker() as sync_session:
            yield sync_session


get_db = DB.get_db
get_sync_db = DB.get_sync_db
