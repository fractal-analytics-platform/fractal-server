import logging

from sqlalchemy import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models import UserSettings
from fractal_server.data_migrations.tools import _check_current_version


def fix_db():
    logger = logging.getLogger("fix_db")
    logger.warning("START execution of fix_db function")
    _check_current_version("2.7.0")

    list_username_or_slurm_user = []
    with next(get_sync_db()) as db:
        stm_users = select(UserOAuth).order_by(UserOAuth.id)
        users = db.execute(stm_users).scalars().unique().all()
        for user in users:
            logger.warning(f"START handling user {user.id}: '{user.email}'")
            user_settings = db.get(UserSettings, user.user_settings_id)
            list_username_or_slurm_user.append(
                user.username or user_settings.slurm_user
            )
            logger.warning(f"END handling user {user.id}: '{user.email}'")
    print(list_username_or_slurm_user)
    logger.warning("END of execution of fix_db function")
