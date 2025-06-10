from pathlib import Path
from tempfile import TemporaryDirectory

from ..utils_background import fail_and_cleanup
from ..utils_background import get_activity_and_task_group
from fractal_server.app.db import get_sync_db
from fractal_server.logger import reset_logger_handlers
from fractal_server.logger import set_logger
from fractal_server.ssh._fabric import SingleUseFractalSSH
from fractal_server.ssh._fabric import SSHConfig
from fractal_server.tasks.utils import get_log_path


def reactivate_ssh_pixi(
    *,
    task_group_activity_id: int,
    task_group_id: int,
    ssh_config: SSHConfig,
    tasks_base_dir: str,
) -> None:
    """
    Reactivate a task group venv.

    This function is run as a background task, therefore exceptions must be
    handled.

    Arguments:
        task_group_id:
        task_group_activity_id:
        ssh_config:
        tasks_base_dir:
            Only used as a `safe_root` in `remove_dir`, and typically set to
            `user_settings.ssh_tasks_dir`.
    """

    LOGGER_NAME = f"{__name__}.ID{task_group_activity_id}"

    with TemporaryDirectory() as tmpdir:
        log_file_path = get_log_path(Path(tmpdir))
        logger = set_logger(
            logger_name=LOGGER_NAME,
            log_file_path=log_file_path,
        )

        with SingleUseFractalSSH(
            ssh_config=ssh_config,
            logger_name=LOGGER_NAME,
        ) as fractal_ssh:

            with next(get_sync_db()) as db:
                success, task_group, activity = get_activity_and_task_group(
                    task_group_activity_id=task_group_activity_id,
                    task_group_id=task_group_id,
                    db=db,
                )
                if not success:
                    return

                # Log some info
                logger.info("START")
                for key, value in task_group.model_dump().items():
                    logger.debug(f"task_group.{key}: {value}")

                # Check that SSH connection works
                try:
                    fractal_ssh.check_connection()
                except Exception as e:
                    logger.error("Cannot establish SSH connection.")
                    fail_and_cleanup(
                        task_group=task_group,
                        task_group_activity=activity,
                        logger_name=LOGGER_NAME,
                        log_file_path=log_file_path,
                        exception=e,
                        db=db,
                    )
                    return

                try:
                    raise NotImplementedError("pixi-task reactivation FIXME")

                    reset_logger_handlers(logger)

                except Exception as reactivate_e:
                    # Delete corrupted venv_path
                    try:
                        logger.info(
                            f"Now delete folder {task_group.venv_path}"
                        )
                        fractal_ssh.remove_folder(
                            folder=task_group.venv_path,
                            safe_root=tasks_base_dir,
                        )
                        logger.info(f"Deleted folder {task_group.venv_path}")
                    except Exception as rm_e:
                        logger.error(
                            "Removing folder failed.\n"
                            f"Original error:\n{str(rm_e)}"
                        )

                    fail_and_cleanup(
                        task_group=task_group,
                        task_group_activity=activity,
                        logger_name=LOGGER_NAME,
                        log_file_path=log_file_path,
                        exception=reactivate_e,
                        db=db,
                    )
