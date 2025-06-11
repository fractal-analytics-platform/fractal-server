from pathlib import Path
from tempfile import TemporaryDirectory

from ..utils_background import add_commit_refresh
from ..utils_background import fail_and_cleanup
from ..utils_background import get_activity_and_task_group
from ..utils_pixi import SOURCE_DIR_NAME
from ._utils import check_ssh_or_fail_and_cleanup
from fractal_server.app.db import get_sync_db
from fractal_server.app.schemas.v2.task_group import TaskGroupActivityStatusV2
from fractal_server.logger import reset_logger_handlers
from fractal_server.logger import set_logger
from fractal_server.ssh._fabric import SingleUseFractalSSH
from fractal_server.ssh._fabric import SSHConfig
from fractal_server.tasks.utils import get_log_path
from fractal_server.tasks.v2.utils_background import get_current_log
from fractal_server.utils import get_timestamp


def deactivate_ssh_pixi(
    *,
    task_group_activity_id: int,
    task_group_id: int,
    ssh_config: SSHConfig,
    tasks_base_dir: str,
) -> None:
    """
    Deactivate a pixi task group venv.

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
        logger.debug("START")
        with next(get_sync_db()) as db:
            db_objects_ok, task_group, activity = get_activity_and_task_group(
                task_group_activity_id=task_group_activity_id,
                task_group_id=task_group_id,
                db=db,
                logger_name=LOGGER_NAME,
            )
            if not db_objects_ok:
                return

            with SingleUseFractalSSH(
                ssh_config=ssh_config,
                logger_name=LOGGER_NAME,
            ) as fractal_ssh:
                try:
                    # Check SSH connection
                    ssh_ok = check_ssh_or_fail_and_cleanup(
                        fractal_ssh=fractal_ssh,
                        task_group=task_group,
                        task_group_activity=activity,
                        logger_name=LOGGER_NAME,
                        log_file_path=log_file_path,
                        db=db,
                    )
                    if not ssh_ok:
                        return

                    # Check that the (remote) task_group venv_path does exist
                    source_dir = Path(
                        task_group.path, SOURCE_DIR_NAME
                    ).as_posix()
                    if not fractal_ssh.remote_exists(source_dir):
                        error_msg = f"{source_dir} does not exist."
                        logger.error(error_msg)
                        fail_and_cleanup(
                            task_group=task_group,
                            task_group_activity=activity,
                            logger_name=LOGGER_NAME,
                            log_file_path=log_file_path,
                            exception=FileNotFoundError(error_msg),
                            db=db,
                        )
                        return

                    # Actually mark the task group as non-active
                    logger.info("Now setting `active=False`.")
                    task_group.active = False
                    task_group = add_commit_refresh(obj=task_group, db=db)

                    # Proceed with deactivation
                    logger.info(f"Now removing {source_dir}.")
                    fractal_ssh.remove_folder(
                        folder=source_dir,
                        safe_root=tasks_base_dir,
                    )
                    logger.info(f"All good, {source_dir} removed.")
                    activity.status = TaskGroupActivityStatusV2.OK
                    activity.log = get_current_log(log_file_path)
                    activity.timestamp_ended = get_timestamp()
                    activity = add_commit_refresh(obj=activity, db=db)

                    reset_logger_handlers(logger)

                except Exception as e:
                    fail_and_cleanup(
                        task_group=task_group,
                        task_group_activity=activity,
                        logger_name=LOGGER_NAME,
                        log_file_path=log_file_path,
                        exception=e,
                        db=db,
                    )
