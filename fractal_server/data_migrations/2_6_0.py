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
        for user in db.execute(select(UserOAuth)).scalars().unique().all():
            user.settings = UserSettings(
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
            db.add(user)
            logger.warning(f"Added UserSettings for User {user.id} ")
        db.commit()
        logger.warning("Commited UserSettings to the database")

    logger.warning("END of execution of fix_db function")
