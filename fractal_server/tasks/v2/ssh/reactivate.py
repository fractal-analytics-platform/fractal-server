import logging
import time
from pathlib import Path
from tempfile import TemporaryDirectory

from ..utils_background import add_commit_refresh
from ..utils_background import fail_and_cleanup
from ..utils_templates import get_collection_replacements
from ._utils import _customize_and_run_template
from fractal_server.app.db import get_sync_db
from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.schemas.v2 import TaskGroupActivityActionV2
from fractal_server.app.schemas.v2.task_group import TaskGroupActivityStatusV2
from fractal_server.logger import reset_logger_handlers
from fractal_server.logger import set_logger
from fractal_server.ssh._fabric import SingleUseFractalSSH
from fractal_server.ssh._fabric import SSHConfig
from fractal_server.tasks.utils import get_log_path
from fractal_server.tasks.v2.utils_background import get_current_log
from fractal_server.tasks.v2.utils_python_interpreter import (
    get_python_interpreter_v2,
)
from fractal_server.tasks.v2.utils_templates import SCRIPTS_SUBFOLDER
from fractal_server.utils import get_timestamp


def reactivate_ssh(
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

                # Get main objects from db
                activity = db.get(TaskGroupActivityV2, task_group_activity_id)
                task_group = db.get(TaskGroupV2, task_group_id)
                if activity is None or task_group is None:
                    # Use `logging` directly
                    logging.error(
                        "Cannot find database rows with "
                        f"{task_group_id=} and {task_group_activity_id=}:\n"
                        f"{task_group=}\n{activity=}. Exit."
                    )
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

                # Check that the (remote) task_group venv_path does not exist
                if fractal_ssh.remote_exists(task_group.venv_path):
                    error_msg = f"{task_group.venv_path} already exists."
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

                try:
                    activity.status = TaskGroupActivityStatusV2.ONGOING
                    activity = add_commit_refresh(obj=activity, db=db)

                    # Prepare replacements for templates
                    replacements = get_collection_replacements(
                        task_group=task_group,
                        python_bin=get_python_interpreter_v2(
                            python_version=task_group.python_version
                        ),
                    )

                    # Prepare replacements for templates
                    pip_freeze_file_local = f"{tmpdir}/pip_freeze.txt"
                    pip_freeze_file_remote = (
                        Path(task_group.path) / "_tmp_pip_freeze.txt"
                    ).as_posix()
                    with open(pip_freeze_file_local, "w") as f:
                        f.write(task_group.pip_freeze)
                    fractal_ssh.send_file(
                        local=pip_freeze_file_local,
                        remote=pip_freeze_file_remote,
                    )
                    replacements.append(
                        ("__PIP_FREEZE_FILE__", pip_freeze_file_remote)
                    )

                    # Define script_dir_remote and create it if missing
                    script_dir_remote = (
                        Path(task_group.path) / SCRIPTS_SUBFOLDER
                    ).as_posix()
                    fractal_ssh.mkdir(folder=script_dir_remote, parents=True)

                    # Prepare common arguments for _customize_and_run_template
                    common_args = dict(
                        replacements=replacements,
                        script_dir_local=(
                            Path(tmpdir) / SCRIPTS_SUBFOLDER
                        ).as_posix(),
                        script_dir_remote=script_dir_remote,
                        prefix=(
                            f"{int(time.time())}_"
                            f"{TaskGroupActivityActionV2.REACTIVATE}"
                        ),
                        fractal_ssh=fractal_ssh,
                        logger_name=LOGGER_NAME,
                    )

                    # Create remote directory for scripts
                    fractal_ssh.mkdir(folder=script_dir_remote)

                    logger.info("start - create venv")
                    _customize_and_run_template(
                        template_filename="1_create_venv.sh",
                        **common_args,
                    )
                    logger.info("end - create venv")
                    activity.log = get_current_log(log_file_path)
                    activity = add_commit_refresh(obj=activity, db=db)

                    logger.info("start - install from pip freeze")
                    _customize_and_run_template(
                        template_filename="6_pip_install_from_freeze.sh",
                        **common_args,
                    )
                    logger.info("end - install from pip freeze")
                    activity.log = get_current_log(log_file_path)
                    activity.status = TaskGroupActivityStatusV2.OK
                    activity.timestamp_ended = get_timestamp()
                    activity = add_commit_refresh(obj=activity, db=db)
                    task_group.active = True
                    task_group = add_commit_refresh(obj=task_group, db=db)
                    logger.info("END")

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
