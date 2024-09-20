import logging

from packaging.version import parse
from sqlalchemy import select

import fractal_server
from fractal_server.app.db import get_sync_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models import UserSettings


def _check_current_version(*, expected_version: str):
    # Check that this module matches with the current version
    module_version = parse(expected_version)
    current_version = parse(fractal_server.__VERSION__)
    if (
        current_version.major != module_version.major
        or current_version.minor != module_version.minor
        or current_version.micro != module_version.micro
    ):
        raise RuntimeError(
            f"{fractal_server.__VERSION__=} not matching with {__file__=}"
        )


def fix_db():
    logger = logging.getLogger("fix_db")
    logger.warning("START execution of fix_db function")
    _check_current_version(expected_version="2.6.0")

    with next(get_sync_db()) as db:
        for user in db.execute(select(UserOAuth)).scalars().unique().all():
            user.settings = UserSettings(
                slurm_user=user.slurm_user,
                slurm_accounts=user.slurm_accounts,
                cache_dir=user.cache_dir,
            )
            db.add(user)
            logger.warning(f"Added UserSettings for User {user.id} ")
        db.commit()
        logger.warning("Commited UserSettings to the database")

    logger.warning("END of execution of fix_db function")
