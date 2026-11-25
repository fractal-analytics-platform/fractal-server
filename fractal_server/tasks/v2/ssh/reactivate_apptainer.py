import time
from pathlib import Path
from tempfile import TemporaryDirectory

from sqlalchemy.exc import NoResultFound

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import Profile
from fractal_server.app.models import Resource
from fractal_server.app.schemas.v2 import TaskGroupActivityAction
from fractal_server.app.schemas.v2 import TaskGroupActivityStatus
from fractal_server.logger import reset_logger_handlers
from fractal_server.logger import set_logger
from fractal_server.ssh._fabric import SingleUseFractalSSH
from fractal_server.ssh._fabric import SSHConfig
from fractal_server.tasks.config import TasksApptainerSettings
from fractal_server.tasks.utils import get_log_path
from fractal_server.tasks.v2.utils_background import add_commit_refresh
from fractal_server.tasks.v2.utils_background import fail_and_cleanup
from fractal_server.tasks.v2.utils_background import get_activity_and_task_group
from fractal_server.tasks.v2.utils_background import get_current_log
from fractal_server.tasks.v2.utils_templates import SCRIPTS_SUBFOLDER
from fractal_server.utils import get_timestamp

from ._pixi_slurm_ssh import run_script_on_remote_slurm
from ._utils import check_ssh_or_fail_and_cleanup
from .collect_apptainer import DEF_FILENAME
from .collect_apptainer import SIF_FILENAME


def reactivate_ssh_apptainer(
    *,
    task_group_activity_id: int,
    task_group_id: int,
    resource: Resource,
    profile: Profile,
) -> None:
    """
    Reactivate an apptainer task group, by re-building its SIF image from the
    existing definition file.

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
        logger.info("START")
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

                    sif_path_remote = Path(
                        task_group.path, SIF_FILENAME
                    ).as_posix()
                    # Check that the (remote) SIF file does not exist
                    if fractal_ssh.remote_exists(sif_path_remote):
                        error_msg = f"{sif_path_remote} already exists."
                        logger.error(error_msg)
                        fail_and_cleanup(
                            task_group=task_group,
                            task_group_activity=activity,
                            logger_name=LOGGER_NAME,
                            log_file_path=log_file_path,
                            exception=FileExistsError(error_msg),
                            db=db,
                        )
                        return

                    # Set status to ONGOING and refresh logs
                    activity.status = TaskGroupActivityStatus.ONGOING
                    activity.log = get_current_log(log_file_path)
                    activity = add_commit_refresh(obj=activity, db=db)

                    # Re-build the image from the existing definition file
                    apptainer_config = TasksApptainerSettings(
                        **resource.tasks_apptainer_config
                    )
                    def_path_remote = Path(
                        task_group.path, DEF_FILENAME
                    ).as_posix()
                    build_command = (
                        f"{apptainer_config.build_env_prefix}"
                        f"{apptainer_config.apptainer_bin} build "
                        f"{sif_path_remote} {def_path_remote}"
                    )
                    logger.info(f"Build command: {build_command}")
                    run_script_on_remote_slurm(
                        job_name=LOGGER_NAME,
                        commands=["date", build_command, "date"],
                        workdir_remote=Path(
                            task_group.path, SCRIPTS_SUBFOLDER
                        ).as_posix(),
                        slurm_config=apptainer_config.SLURM_CONFIG.model_dump(),
                        fractal_ssh=fractal_ssh,
                        logger_name=LOGGER_NAME,
                        prefix=(
                            f"{int(time.time())}_"
                            f"{TaskGroupActivityAction.REACTIVATE}"
                        ),
                        db=db,
                        activity=activity,
                        log_file_path=log_file_path,
                        poll_interval=resource.jobs_poll_interval,
                    )

                    # Finalize (write metadata to DB)
                    activity.status = TaskGroupActivityStatus.OK
                    activity.log = get_current_log(log_file_path)
                    activity.timestamp_ended = get_timestamp()
                    activity = add_commit_refresh(obj=activity, db=db)
                    task_group.active = True
                    task_group = add_commit_refresh(obj=task_group, db=db)
                    logger.info("END")

                    reset_logger_handlers(logger)

                except Exception as reactivate_e:
                    # Delete corrupted SIF file, if any
                    try:
                        logger.info(f"Now delete {sif_path_remote}")
                        fractal_ssh.run_command(cmd=f"rm -f {sif_path_remote}")
                        logger.info(f"Deleted {sif_path_remote}")
                    except Exception as rm_e:
                        logger.error(
                            f"Removing {sif_path_remote} failed. "
                            f"Original error: {str(rm_e)}"
                        )
                    fail_and_cleanup(
                        task_group=task_group,
                        task_group_activity=activity,
                        logger_name=LOGGER_NAME,
                        log_file_path=log_file_path,
                        exception=reactivate_e,
                        db=db,
                    )
