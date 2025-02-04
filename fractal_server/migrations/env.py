from logging.config import fileConfig

from alembic import context
from sqlmodel import SQLModel

from fractal_server.migrations.naming_convention import NAMING_CONVENTION

# Alembic Config object (provides access to the values within the .ini file)
config = context.config


# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)


target_metadata = SQLModel.metadata
target_metadata.naming_convention = NAMING_CONVENTION
# Importing `fractal_server.app.models` *after* defining
# `SQLModel.metadata.naming_convention` in order to apply the naming convention
# when autogenerating migrations (see issue #1819).
from fractal_server.app import models  # noqa


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
