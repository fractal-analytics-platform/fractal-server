import logging
import os
import time
from pathlib import Path
from tempfile import TemporaryDirectory

from ..utils_background import _prepare_tasks_metadata
from ..utils_background import fail_and_cleanup
from ..utils_database import create_db_tasks_and_update_task_group
from fractal_server.app.db import get_sync_db
from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.schemas.v2 import TaskGroupActivityActionV2
from fractal_server.app.schemas.v2 import TaskGroupActivityStatusV2
from fractal_server.app.schemas.v2.manifest import ManifestV2
from fractal_server.logger import get_logger
from fractal_server.logger import set_logger
from fractal_server.ssh._fabric import FractalSSH
from fractal_server.tasks.v2.utils_background import add_commit_refresh
from fractal_server.tasks.v2.utils_background import get_current_log
from fractal_server.tasks.v2.utils_package_names import compare_package_names
from fractal_server.tasks.v2.utils_python_interpreter import (
    get_python_interpreter_v2,
)
from fractal_server.tasks.v2.utils_templates import customize_template
from fractal_server.tasks.v2.utils_templates import get_collection_replacements
from fractal_server.tasks.v2.utils_templates import (
    parse_script_pip_show_stdout,
)
from fractal_server.tasks.v2.utils_templates import SCRIPTS_SUBFOLDER
from fractal_server.utils import get_timestamp

LOGGER_NAME = __name__


def _customize_and_run_template(
    *,
    template_filename: str,
    replacements: list[tuple[str, str]],
    script_dir_local: str,
    prefix: str,
    fractal_ssh: FractalSSH,
    script_dir_remote: str,
    logger_name: str,
) -> str:
    """
    Customize one of the template bash scripts, transfer it to the remote host
    via SFTP and then run it via SSH.

    Args:
        template_filename: Filename of the template file (ends with ".sh").
        replacements: Dictionary of replacements.
        script_dir: Local folder where the script will be placed.
        prefix: Prefix for the script filename.
        fractal_ssh: FractalSSH object
        script_dir_remote: Remote scripts directory
    """
    logger = get_logger(logger_name=logger_name)
    logger.debug(f"_customize_and_run_template {template_filename} - START")

    # Prepare name and path of script
    if not template_filename.endswith(".sh"):
        raise ValueError(
            f"Invalid {template_filename=} (it must end with '.sh')."
        )
    script_filename = f"{prefix}_{template_filename}"
    script_path_local = Path(script_dir_local) / script_filename

    customize_template(
        template_name=template_filename,
        replacements=replacements,
        script_path=script_path_local,
    )

    # Transfer script to remote host
    script_path_remote = os.path.join(
        script_dir_remote,
        script_filename,
    )
    logger.debug(f"Now transfer {script_path_local=} over SSH.")
    fractal_ssh.send_file(
        local=script_path_local,
        remote=script_path_remote,
    )

    # Execute script remotely
    cmd = f"bash {script_path_remote}"
    logger.debug(f"Now run '{cmd}' over SSH.")
    stdout = fractal_ssh.run_command(cmd=cmd)
    logger.debug(f"Standard output of '{cmd}':\n{stdout}")

    logger.debug(f"_customize_and_run_template {template_filename} - END")
    return stdout


def _copy_wheel_file_ssh(
    task_group: TaskGroupV2, fractal_ssh: FractalSSH
) -> str:
    logger = get_logger(LOGGER_NAME)
    source = task_group.wheel_path
    dest = (
        Path(task_group.path) / Path(task_group.wheel_path).name
    ).as_posix()
    cmd = f"cp {source} {dest}"
    logger.debug(f"[_copy_wheel_file] START {source=} {dest=}")
    fractal_ssh.run_command(cmd=cmd)
    logger.debug(f"[_copy_wheel_file] END {source=} {dest=}")
    return dest


def collect_ssh(
    *,
    task_group_id: int,
    task_group_activity_id: int,
    fractal_ssh: FractalSSH,
    tasks_base_dir: str,
) -> None:
    """
    Collect a task package over SSH

    This function is run as a background task, therefore exceptions must be
    handled.

    NOTE: by making this function sync, it runs within a thread - due to
    starlette/fastapi handling of background tasks (see
    https://github.com/encode/starlette/blob/master/starlette/background.py).


    Arguments:
        task_group_id:
        task_group_activity_id:
        fractal_ssh:
        tasks_base_dir:
            Only used as a `safe_root` in `remove_dir`, and typically set to
            `user_settings.ssh_tasks_dir`.
    """

    # Work within a temporary folder, where also logs will be placed
    with TemporaryDirectory() as tmpdir:
        LOGGER_NAME = "task_collection_ssh"
        log_file_path = Path(tmpdir) / "log"
        logger = set_logger(
            logger_name=LOGGER_NAME,
            log_file_path=log_file_path,
        )

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
            logger.debug("START")
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

            # Check that the (remote) task_group path does not exist
            if fractal_ssh.remote_exists(task_group.path):
                error_msg = f"{task_group.path} already exists."
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

                # Prepare replacements for templates
                replacements = get_collection_replacements(
                    task_group=task_group,
                    python_bin=get_python_interpreter_v2(
                        python_version=task_group.python_version
                    ),
                )

                # Prepare common arguments for `_customize_and_run_template``
                script_dir_remote = (
                    Path(task_group.path) / SCRIPTS_SUBFOLDER
                ).as_posix()
                common_args = dict(
                    replacements=replacements,
                    script_dir_local=(
                        Path(tmpdir) / SCRIPTS_SUBFOLDER
                    ).as_posix(),
                    script_dir_remote=script_dir_remote,
                    prefix=(
                        f"{int(time.time())}_"
                        f"{TaskGroupActivityActionV2.COLLECT}_"
                    ),
                    fractal_ssh=fractal_ssh,
                    logger_name=LOGGER_NAME,
                )

                # Create remote `task_group.path` and `script_dir_remote`
                # folders (note that because of `parents=True` we  are in
                # the `no error if existing, make parent directories as
                # needed` scenario for `mkdir`)
                fractal_ssh.mkdir(folder=task_group.path, parents=True)
                fractal_ssh.mkdir(folder=script_dir_remote, parents=True)

                # Copy wheel file into task group path
                if task_group.wheel_path:
                    new_wheel_path = _copy_wheel_file_ssh(
                        task_group=task_group,
                        fractal_ssh=fractal_ssh,
                    )
                    task_group.wheel_path = new_wheel_path
                    task_group = add_commit_refresh(obj=task_group, db=db)

                logger.debug("installing - START")

                # Set status to ONGOING and refresh logs
                activity.status = TaskGroupActivityStatusV2.ONGOING
                activity.log = get_current_log(log_file_path)
                activity = add_commit_refresh(obj=activity, db=db)

                # Run script 1
                stdout = _customize_and_run_template(
                    template_filename="1_create_venv.sh",
                    **common_args,
                )
                activity.log = get_current_log(log_file_path)
                activity = add_commit_refresh(obj=activity, db=db)

                # Run script 2
                stdout = _customize_and_run_template(
                    template_filename="2_pip_install.sh",
                    **common_args,
                )
                activity.log = get_current_log(log_file_path)
                activity = add_commit_refresh(obj=activity, db=db)

                # Run script 3
                pip_freeze_stdout = _customize_and_run_template(
                    template_filename="3_pip_freeze.sh",
                    **common_args,
                )
                activity.log = get_current_log(log_file_path)
                activity = add_commit_refresh(obj=activity, db=db)

                # Run script 4
                stdout = _customize_and_run_template(
                    template_filename="4_pip_show.sh",
                    **common_args,
                )
                activity.log = get_current_log(log_file_path)
                activity = add_commit_refresh(obj=activity, db=db)

                # Run script 5
                venv_info = _customize_and_run_template(
                    template_filename="5_get_venv_size_and_file_number.sh",
                    **common_args,
                )
                venv_size, venv_file_number = venv_info.split()
                activity.log = get_current_log(log_file_path)
                activity = add_commit_refresh(obj=activity, db=db)

                pkg_attrs = parse_script_pip_show_stdout(stdout)

                for key, value in pkg_attrs.items():
                    logger.debug(f"parsed from pip-show: {key}={value}")
                # Check package_name match between pip show and task-group
                package_name_pip_show = pkg_attrs.get("package_name")
                package_name_task_group = task_group.pkg_name
                compare_package_names(
                    pkg_name_pip_show=package_name_pip_show,
                    pkg_name_task_group=package_name_task_group,
                    logger_name=LOGGER_NAME,
                )
                # Extract/drop parsed attributes
                package_name = package_name_task_group
                python_bin = pkg_attrs.pop("python_bin")
                package_root_parent_remote = pkg_attrs.pop(
                    "package_root_parent"
                )
                manifest_path_remote = pkg_attrs.pop("manifest_path")

                # TODO SSH: Use more robust logic to determine `package_root`.
                # Examples: use `importlib.util.find_spec`, or parse the output
                # of `pip show --files {package_name}`.
                package_name_underscore = package_name.replace("-", "_")
                package_root_remote = (
                    Path(package_root_parent_remote) / package_name_underscore
                ).as_posix()

                # Read and validate remote manifest file
                pkg_manifest_dict = fractal_ssh.read_remote_json_file(
                    manifest_path_remote
                )
                logger.info(f"Loaded {manifest_path_remote=}")
                pkg_manifest = ManifestV2(**pkg_manifest_dict)
                logger.info("Manifest is a valid ManifestV2")

                logger.info("_prepare_tasks_metadata - start")
                task_list = _prepare_tasks_metadata(
                    package_manifest=pkg_manifest,
                    package_version=task_group.version,
                    package_root=Path(package_root_remote),
                    python_bin=Path(python_bin),
                )
                logger.info("_prepare_tasks_metadata - end")

                logger.info("create_db_tasks_and_update_task_group - " "start")
                create_db_tasks_and_update_task_group(
                    task_list=task_list,
                    task_group_id=task_group.id,
                    db=db,
                )
                logger.info("create_db_tasks_and_update_task_group - end")

                # Update task_group data
                logger.info(
                    "Add pip_freeze, venv_size and venv_file_number "
                    "to TaskGroupV2 - start"
                )
                task_group.pip_freeze = pip_freeze_stdout
                task_group.venv_size_in_kB = int(venv_size)
                task_group.venv_file_number = int(venv_file_number)
                task_group = add_commit_refresh(obj=task_group, db=db)
                logger.info(
                    "Add pip_freeze, venv_size and venv_file_number "
                    "to TaskGroupV2 - end"
                )

                # Finalize (write metadata to DB)
                logger.debug("finalising - START")
                activity.status = TaskGroupActivityStatusV2.OK
                activity.timestamp_ended = get_timestamp()
                activity = add_commit_refresh(obj=activity, db=db)
                logger.debug("finalising - END")
                logger.debug("END")

                logger.debug("END")

            except Exception as collection_e:
                # Delete corrupted package dir
                try:
                    logger.info(f"Now delete remote folder {task_group.path}")
                    fractal_ssh.remove_folder(
                        folder=task_group.path,
                        safe_root=tasks_base_dir,
                    )
                    logger.info(f"Deleted remoted folder {task_group.path}")
                except Exception as e_rm:
                    logger.error(
                        "Removing folder failed. "
                        f"Original error:\n{str(e_rm)}"
                    )
                fail_and_cleanup(
                    task_group=task_group,
                    task_group_activity=activity,
                    log_file_path=log_file_path,
                    logger_name=LOGGER_NAME,
                    exception=collection_e,
                    db=db,
                )
    return
