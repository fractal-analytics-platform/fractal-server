from pathlib import Path
from tempfile import TemporaryDirectory

from ..utils_background import add_commit_refresh
from ..utils_background import fail_and_cleanup
from ..utils_background import get_activity_and_task_group
from ._utils import check_ssh_or_fail_and_cleanup
from fractal_server.app.db import get_sync_db
from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.schemas.v2 import TaskGroupActivityActionV2
from fractal_server.app.schemas.v2 import TaskGroupActivityStatusV2
from fractal_server.app.schemas.v2 import TaskGroupV2OriginEnum
from fractal_server.logger import reset_logger_handlers
from fractal_server.logger import set_logger
from fractal_server.ssh._fabric import SingleUseFractalSSH
from fractal_server.ssh._fabric import SSHConfig
from fractal_server.tasks.utils import get_log_path
from fractal_server.tasks.v2.utils_background import get_current_log
from fractal_server.utils import get_timestamp


def delete_ssh(
    *,
    task_group_activity_id: int,
    task_group_id: int,
    ssh_config: SSHConfig,
    tasks_base_dir: str,
) -> None:
    """
    Delete a task group.

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

            if task_group.origin == TaskGroupV2OriginEnum.OTHER:
                task_group_activity = TaskGroupActivityV2(
                    user_id=task_group.user_id,
                    taskgroupv2_id=task_group.id,
                    status=TaskGroupActivityStatusV2.OK,
                    action=TaskGroupActivityActionV2.DELETE,
                    pkg_name=task_group.pkg_name,
                    version=(task_group.version or "N/A"),
                    timestamp_started=get_timestamp(),
                    timestamp_ended=get_timestamp(),
                )
                db.add(task_group_activity)
                db.delete(task_group)
                db.commit()
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

                    # Check that the (local) task_group path does exist
                    if not fractal_ssh.remote_exists(task_group.path):
                        error_msg = f"{task_group.path} does not exist."
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

                    activity.status = TaskGroupActivityStatusV2.ONGOING
                    activity = add_commit_refresh(obj=activity, db=db)

                    # Proceed with removal
                    logger.info(f"Now removing {task_group.path}.")
                    db.delete(task_group)
                    db.commit()
                    if task_group.origin != TaskGroupV2OriginEnum.OTHER:
                        fractal_ssh.remove_folder(
                            folder=task_group.path,
                            safe_root=tasks_base_dir,
                        )
                        logger.info(f"All good, {task_group.path} removed.")

                    activity.status = TaskGroupActivityStatusV2.OK
                    activity.log = get_current_log(log_file_path)
                    activity.timestamp_ended = get_timestamp()
                    activity = add_commit_refresh(obj=activity, db=db)

                    logger.debug("END")
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
