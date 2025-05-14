import logging
import time
from pathlib import Path
from tempfile import TemporaryDirectory

from ....ssh._fabric import SingleUseFractalSSH
from ..utils_background import _prepare_tasks_metadata
from ..utils_background import fail_and_cleanup
from ..utils_database import create_db_tasks_and_update_task_group_sync
from fractal_server.app.db import get_sync_db
from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.schemas.v2 import TaskGroupActivityActionV2
from fractal_server.app.schemas.v2 import TaskGroupActivityStatusV2
from fractal_server.app.schemas.v2 import WheelFile
from fractal_server.app.schemas.v2.manifest import ManifestV2
from fractal_server.logger import reset_logger_handlers
from fractal_server.logger import set_logger
from fractal_server.ssh._fabric import SSHConfig
from fractal_server.tasks.v2.ssh._utils import _customize_and_run_template
from fractal_server.tasks.v2.utils_background import add_commit_refresh
from fractal_server.tasks.v2.utils_background import get_current_log
from fractal_server.tasks.v2.utils_package_names import compare_package_names
from fractal_server.tasks.v2.utils_python_interpreter import (
    get_python_interpreter_v2,
)
from fractal_server.tasks.v2.utils_templates import get_collection_replacements
from fractal_server.tasks.v2.utils_templates import (
    parse_script_pip_show_stdout,
)
from fractal_server.tasks.v2.utils_templates import SCRIPTS_SUBFOLDER
from fractal_server.utils import get_timestamp


def collect_ssh(
    *,
    task_group_id: int,
    task_group_activity_id: int,
    ssh_config: SSHConfig,
    tasks_base_dir: str,
    wheel_file: WheelFile | None = None,
) -> None:
    """
    Collect a task package over SSH

    This function runs as a background task, therefore exceptions must be
    handled.

    NOTE: since this function is sync, it runs within a thread - due to
    starlette/fastapi handling of background tasks (see
    https://github.com/encode/starlette/blob/master/starlette/background.py).


    Arguments:
        task_group_id:
        task_group_activity_id:
        ssh_config:
        tasks_base_dir:
            Only used as a `safe_root` in `remove_dir`, and typically set to
            `user_settings.ssh_tasks_dir`.
        wheel_file:
    """

    LOGGER_NAME = f"{__name__}.ID{task_group_activity_id}"

    # Work within a temporary folder, where also logs will be placed
    with TemporaryDirectory() as tmpdir:
        log_file_path = Path(tmpdir) / "log"
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
                    # Create remote `task_group.path` and `script_dir_remote`
                    # folders (note that because of `parents=True` we  are in
                    # the `no error if existing, make parent directories as
                    # needed` scenario for `mkdir`)
                    script_dir_remote = (
                        Path(task_group.path) / SCRIPTS_SUBFOLDER
                    ).as_posix()
                    fractal_ssh.mkdir(folder=task_group.path, parents=True)
                    fractal_ssh.mkdir(folder=script_dir_remote, parents=True)

                    # Write wheel file locally and send it to remote path,
                    # and set task_group.wheel_path
                    if wheel_file is not None:
                        wheel_filename = wheel_file.filename
                        wheel_path = (
                            Path(task_group.path) / wheel_filename
                        ).as_posix()
                        tmp_wheel_path = (
                            Path(tmpdir) / wheel_filename
                        ).as_posix()
                        logger.info(
                            f"Write wheel-file contents into {tmp_wheel_path}"
                        )
                        with open(tmp_wheel_path, "wb") as f:
                            f.write(wheel_file.contents)
                        fractal_ssh.send_file(
                            local=tmp_wheel_path,
                            remote=wheel_path,
                        )
                        task_group.wheel_path = wheel_path
                        task_group = add_commit_refresh(obj=task_group, db=db)

                    replacements = get_collection_replacements(
                        task_group=task_group,
                        python_bin=get_python_interpreter_v2(
                            python_version=task_group.python_version
                        ),
                    )

                    # Prepare common arguments for _customize_and_run_template
                    common_args = dict(
                        replacements=replacements,
                        script_dir_local=(
                            Path(tmpdir) / SCRIPTS_SUBFOLDER
                        ).as_posix(),
                        script_dir_remote=script_dir_remote,
                        prefix=(
                            f"{int(time.time())}_"
                            f"{TaskGroupActivityActionV2.COLLECT}"
                        ),
                        fractal_ssh=fractal_ssh,
                        logger_name=LOGGER_NAME,
                    )

                    logger.info("installing - START")

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

                    # TODO SSH: Use more robust logic to determine
                    # `package_root`. Examples: use `importlib.util.find_spec`
                    # or parse the output of `pip show --files {package_name}`.
                    package_name_underscore = package_name.replace("-", "_")
                    package_root_remote = (
                        Path(package_root_parent_remote)
                        / package_name_underscore
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

                    logger.info(
                        "create_db_tasks_and_update_task_group - " "start"
                    )
                    create_db_tasks_and_update_task_group_sync(
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
                    logger.info("finalising - START")
                    activity.status = TaskGroupActivityStatusV2.OK
                    activity.timestamp_ended = get_timestamp()
                    activity = add_commit_refresh(obj=activity, db=db)
                    logger.info("finalising - END")
                    logger.info("END")
                    reset_logger_handlers(logger)

                except Exception as collection_e:
                    # Delete corrupted package dir
                    try:
                        logger.info(
                            f"Now delete remote folder {task_group.path}"
                        )
                        fractal_ssh.remove_folder(
                            folder=task_group.path,
                            safe_root=tasks_base_dir,
                        )
                        logger.info(
                            f"Deleted remoted folder {task_group.path}"
                        )
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
