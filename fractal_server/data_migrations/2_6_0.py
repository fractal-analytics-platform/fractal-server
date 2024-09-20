import logging

from sqlalchemy import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models import UserSettings
from fractal_server.data_migrations.tools import _check_current_version


def fix_db():
    logger = logging.getLogger("fix_db")
    logger.warning("START execution of fix_db function")
    _check_current_version("2.6.0")

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
