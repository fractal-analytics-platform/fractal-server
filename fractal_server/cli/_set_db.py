from pathlib import Path

import alembic.config

import fractal_server
from fractal_server.config import get_db_settings
from fractal_server.syringe import Inject


def set_db() -> None:
    """
    Upgrade database schemas.

    Call alembic to upgrade to the latest migration.
    Ref: https://stackoverflow.com/a/56683030/283972
    """

    # Validate DB settings
    Inject(get_db_settings)

    # Perform migrations
    alembic_ini = Path(fractal_server.__file__).parent / "alembic.ini"
    alembic_args = ["-c", alembic_ini.as_posix(), "upgrade", "head"]
    print(f"START: Run alembic.config, with argv={alembic_args}")
    alembic.config.main(argv=alembic_args)
    print("END: alembic.config")
