from logging.config import fileConfig

from alembic import context
from sqlalchemy.engine import Connection
from sqlmodel import SQLModel

from fractal_server.migrations.naming_convention import NAMING_CONVENTION

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config


# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)


# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = SQLModel.metadata
target_metadata.naming_convention = NAMING_CONVENTION
# Importing `fractal_server.app.models` after defining
# `SQLModel.metadata.naming_convention` in order to apply the naming convention
# when autogenerating migrations (see issue #1819).
from fractal_server.app import models  # noqa

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def do_run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        render_as_batch=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    from fractal_server.app.db import DB

    engine = DB.engine_sync()
    with engine.connect() as connection:
        do_run_migrations(connection=connection)

    engine.dispose()


run_migrations_online()
