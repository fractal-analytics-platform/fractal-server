import logging

from sqlalchemy import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models import UserSettings
from fractal_server.config import get_settings
from fractal_server.data_migrations.tools import _check_current_version
from fractal_server.syringe import Inject


def fix_db():
    logger = logging.getLogger("fix_db")
    logger.warning("START execution of fix_db function")
    _check_current_version("2.6.0")

    global_settings = Inject(get_settings)

    with next(get_sync_db()) as db:
        users = db.execute(select(UserOAuth)).scalars().unique().all()
        for user in sorted(users, key=lambda x: x.id):
            logger.warning(f"START handling user {user.id}: '{user.email}'")
            user_settings = UserSettings(
                # SSH
                ssh_host=global_settings.FRACTAL_SLURM_SSH_HOST,
                ssh_username=global_settings.FRACTAL_SLURM_SSH_USER,
                ssh_private_key_path=(
                    global_settings.FRACTAL_SLURM_SSH_PRIVATE_KEY_PATH
                ),
                ssh_tasks_dir=(
                    global_settings.FRACTAL_SLURM_SSH_WORKING_BASE_DIR
                ),
                ssh_jobs_dir=(
                    global_settings.FRACTAL_SLURM_SSH_WORKING_BASE_DIR
                ),
                # SUDO
                slurm_user=user.slurm_user,
                slurm_accounts=user.slurm_accounts,
                cache_dir=user.cache_dir,
            )
            user.settings = user_settings
            db.add(user)
            db.commit()
            db.refresh(user)
            logger.warning(f"New user {user.id} settings:\n{user.settings}")
            logger.warning(f"END handling user {user.id}: '{user.email}'")

    logger.warning("END of execution of fix_db function")
