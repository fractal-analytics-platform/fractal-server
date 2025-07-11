import time
from pathlib import Path
from tempfile import TemporaryDirectory

from ..utils_background import add_commit_refresh
from ..utils_background import fail_and_cleanup
from ..utils_background import get_activity_and_task_group
from ..utils_templates import get_collection_replacements
from ._utils import _copy_wheel_file_ssh
from ._utils import _customize_and_run_template
from ._utils import check_ssh_or_fail_and_cleanup
from fractal_server.app.db import get_sync_db
from fractal_server.app.schemas.v2 import TaskGroupActivityActionV2
from fractal_server.app.schemas.v2 import TaskGroupV2OriginEnum
from fractal_server.app.schemas.v2.task_group import TaskGroupActivityStatusV2
from fractal_server.logger import reset_logger_handlers
from fractal_server.logger import set_logger
from fractal_server.ssh._fabric import SingleUseFractalSSH
from fractal_server.ssh._fabric import SSHConfig
from fractal_server.tasks.utils import FORBIDDEN_DEPENDENCY_STRINGS
from fractal_server.tasks.utils import get_log_path
from fractal_server.tasks.v2.utils_background import get_current_log
from fractal_server.tasks.v2.utils_templates import SCRIPTS_SUBFOLDER
from fractal_server.utils import get_timestamp


def deactivate_ssh(
    *,
    task_group_activity_id: int,
    task_group_id: int,
    ssh_config: SSHConfig,
    tasks_base_dir: str,
) -> None:
    """
    Deactivate a task group venv.

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

                    # Check that the (local) task_group venv_path does exist
                    if not fractal_ssh.remote_exists(task_group.venv_path):
                        error_msg = f"{task_group.venv_path} does not exist."
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

                    if task_group.env_info is None:
                        logger.warning(
                            "Recreate pip-freeze information, since "
                            f"{task_group.env_info=}. NOTE: this should "
                            "only happen for task groups created before 2.9.0."
                        )

                        # Prepare replacements for templates
                        replacements = get_collection_replacements(
                            task_group=task_group,
                            python_bin="/not/applicable",
                        )

                        # Define script_dir_remote and create it if missing
                        script_dir_remote = (
                            Path(task_group.path) / SCRIPTS_SUBFOLDER
                        ).as_posix()
                        fractal_ssh.mkdir(
                            folder=script_dir_remote, parents=True
                        )

                        # Prepare arguments for `_customize_and_run_template`
                        common_args = dict(
                            replacements=replacements,
                            script_dir_local=(
                                Path(tmpdir) / SCRIPTS_SUBFOLDER
                            ).as_posix(),
                            script_dir_remote=script_dir_remote,
                            prefix=(
                                f"{int(time.time())}_"
                                f"{TaskGroupActivityActionV2.DEACTIVATE}"
                            ),
                            fractal_ssh=fractal_ssh,
                            logger_name=LOGGER_NAME,
                        )

                        # Run `pip freeze`
                        pip_freeze_stdout = _customize_and_run_template(
                            template_filename="3_pip_freeze.sh",
                            **common_args,
                        )

                        # Update pip-freeze data
                        logger.info(
                            "Add pip freeze stdout to TaskGroupV2 - start"
                        )
                        activity.log = get_current_log(log_file_path)
                        activity = add_commit_refresh(obj=activity, db=db)
                        task_group.env_info = pip_freeze_stdout
                        task_group = add_commit_refresh(obj=task_group, db=db)
                        logger.info(
                            "Add pip freeze stdout to TaskGroupV2 - end"
                        )

                    # Handle some specific cases for wheel-file case
                    if task_group.origin == TaskGroupV2OriginEnum.WHEELFILE:
                        logger.info(
                            f"Handle specific cases for {task_group.origin=}."
                        )

                        # Blocking situation: `archive_path` is not set or
                        # points to a missing path
                        if (
                            task_group.archive_path is None
                            or not fractal_ssh.remote_exists(
                                task_group.archive_path
                            )
                        ):
                            error_msg = (
                                "Invalid wheel path for task group with "
                                f"{task_group_id=}. "
                                f"{task_group.archive_path=}  is unset or "
                                "does not exist."
                            )
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

                        # Recoverable situation: `archive_path` was not yet
                        # copied over to the correct server-side folder
                        archive_path_parent_dir = Path(
                            task_group.archive_path
                        ).parent
                        if archive_path_parent_dir != Path(task_group.path):
                            logger.warning(
                                f"{archive_path_parent_dir.as_posix()} "
                                f"differs from {task_group.path}. "
                                "NOTE: this should only happen for task "
                                "groups created before 2.9.0."
                            )

                            if (
                                task_group.archive_path
                                not in task_group.env_info
                            ):
                                raise ValueError(
                                    f"Cannot find {task_group.archive_path=} "
                                    "in pip-freeze data. Exit."
                                )

                            logger.info(
                                f"Now copy wheel file into {task_group.path}."
                            )
                            new_archive_path = _copy_wheel_file_ssh(
                                task_group=task_group,
                                fractal_ssh=fractal_ssh,
                                logger_name=LOGGER_NAME,
                            )
                            logger.info(
                                f"Copied wheel file to {new_archive_path}."
                            )

                            task_group.archive_path = new_archive_path
                            new_pip_freeze = task_group.env_info.replace(
                                task_group.archive_path,
                                new_archive_path,
                            )
                            task_group.env_info = new_pip_freeze
                            task_group = add_commit_refresh(
                                obj=task_group, db=db
                            )
                            logger.info(
                                "Updated `archive_path` and `env_info` "
                                "task-group attributes."
                            )

                    # Fail if `env_info` includes "github", see
                    # https://github.com/fractal-analytics-platform/fractal-server/issues/2142
                    for forbidden_string in FORBIDDEN_DEPENDENCY_STRINGS:
                        if forbidden_string in task_group.env_info:
                            raise ValueError(
                                "Deactivation and reactivation of task "
                                f"packages with direct {forbidden_string} "
                                "dependencies are not currently supported. "
                                "Exit."
                            )

                    # We now have all required information for reactivating the
                    # virtual environment at a later point

                    # Actually mark the task group as non-active
                    logger.info("Now setting `active=False`.")
                    task_group.active = False
                    task_group = add_commit_refresh(obj=task_group, db=db)

                    # Proceed with deactivation
                    logger.info(f"Now removing {task_group.venv_path}.")
                    fractal_ssh.remove_folder(
                        folder=task_group.venv_path,
                        safe_root=tasks_base_dir,
                    )
                    logger.info(f"All good, {task_group.venv_path} removed.")
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
