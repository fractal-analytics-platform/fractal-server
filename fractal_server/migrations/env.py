from logging.config import fileConfig

from alembic import context

from fractal_server.app.models.base import Base

# Alembic Config object (provides access to the values within the .ini file)
config = context.config


# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)


# Import `fractal_server.app.models` to register all table classes on
# `Base.metadata` (the naming convention is already set on `Base.metadata`
# at class-definition time, see `fractal_server/app/models/base.py`).
from fractal_server.app import models  # noqa

target_metadata = Base.metadata


def run_migrations_online() -> None:
    """
    Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.
    """
    from fractal_server.app.db import DB

    engine = DB.engine_sync()
    with engine.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            render_as_batch=True,
        )

        with context.begin_transaction():
            context.run_migrations()

    engine.dispose()


run_migrations_online()
