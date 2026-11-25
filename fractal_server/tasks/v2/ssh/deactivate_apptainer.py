from pathlib import Path
from tempfile import TemporaryDirectory

from sqlalchemy.exc import NoResultFound

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import Profile
from fractal_server.app.models import Resource
from fractal_server.app.schemas.v2.task_group import TaskGroupActivityStatus
from fractal_server.logger import reset_logger_handlers
from fractal_server.logger import set_logger
from fractal_server.ssh._fabric import SingleUseFractalSSH
from fractal_server.ssh._fabric import SSHConfig
from fractal_server.tasks.utils import get_log_path
from fractal_server.tasks.v2.utils_background import add_commit_refresh
from fractal_server.tasks.v2.utils_background import fail_and_cleanup
from fractal_server.tasks.v2.utils_background import get_activity_and_task_group
from fractal_server.tasks.v2.utils_background import get_current_log
from fractal_server.utils import get_timestamp

from ._utils import check_ssh_or_fail_and_cleanup
from .collect_apptainer import SIF_FILENAME


def deactivate_ssh_apptainer(
    *,
    task_group_activity_id: int,
    task_group_id: int,
    resource: Resource,
    profile: Profile,
) -> None:
    """
    Deactivate an apptainer task group, by removing its SIF image.

    This function is run as a background task, therefore exceptions must be
    handled.

    Args:
        task_group_activity_id:
        task_group_id:
        resource:
        profile:
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
            try:
                task_group, activity = get_activity_and_task_group(
                    task_group_activity_id=task_group_activity_id,
                    task_group_id=task_group_id,
                    db=db,
                    logger_name=LOGGER_NAME,
                )
            except NoResultFound:
                return

            with SingleUseFractalSSH(
                ssh_config=SSHConfig(
                    host=resource.host,
                    user=profile.username,
                    key_path=profile.ssh_key_path,
                ),
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

                    # Check that the (remote) SIF file does exist
                    sif_path_remote = Path(
                        task_group.path, SIF_FILENAME
                    ).as_posix()
                    if not fractal_ssh.remote_exists(sif_path_remote):
                        error_msg = f"{sif_path_remote} does not exist."
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
                    logger.info(f"Now removing {sif_path_remote}.")
                    fractal_ssh.run_command(cmd=f"rm {sif_path_remote}")
                    logger.info(f"All good, {sif_path_remote} removed.")
                    activity.status = TaskGroupActivityStatus.OK
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
