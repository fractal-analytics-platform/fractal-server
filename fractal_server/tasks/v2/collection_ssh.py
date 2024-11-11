import os
import time
from pathlib import Path
from tempfile import TemporaryDirectory

from .database_operations import create_db_tasks_and_update_task_group
from .database_operations import update_task_group_pip_freeze
from .utils_background import _handle_failure
from .utils_background import _prepare_tasks_metadata
from .utils_background import _set_task_group_activity_status
from fractal_server.app.db import get_sync_db
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.schemas.v2 import TaskGroupActivityActionV2
from fractal_server.app.schemas.v2 import TaskGroupActivityStatusV2
from fractal_server.app.schemas.v2.manifest import ManifestV2
from fractal_server.config import get_settings
from fractal_server.logger import get_logger
from fractal_server.logger import set_logger
from fractal_server.ssh._fabric import FractalSSH
from fractal_server.syringe import Inject
from fractal_server.tasks.v2.utils_background import _refresh_logs
from fractal_server.tasks.v2.utils_package_names import compare_package_names
from fractal_server.tasks.v2.utils_python_interpreter import (
    get_python_interpreter_v2,
)
from fractal_server.tasks.v2.utils_templates import customize_template
from fractal_server.tasks.v2.utils_templates import parse_script_pip_show
from fractal_server.tasks.v2.utils_templates import SCRIPTS_SUBFOLDER


def _customize_and_run_template(
    *,
    template_filename: str,
    replacements: list[tuple[str, str]],
    script_dir_local: str,
    logger_name: str,
    prefix: str,
    fractal_ssh: FractalSSH,
    script_dir_remote: str,
) -> str:
    """
    Customize one of the template bash scripts, transfer it to the remote host
    via SFTP and then run it via SSH.

    Args:
        template_filename: Filename of the template file (ends with ".sh").
        replacements: Dictionary of replacements.
        script_dir: Local folder where the script will be placed.
        prefix: Prefix for the script filename.
        logger_name: Logger name
        fractal_ssh: FractalSSH object
        script_dir_remote: Remote scripts directory
    """
    logger = get_logger(logger_name)
    logger.debug(f"_customize_and_run_template {template_filename} - START")

    # Prepare name and path of script
    if not template_filename.endswith(".sh"):
        raise ValueError(
            f"Invalid {template_filename=} (it must end with '.sh')."
        )
    template_filename_stripped = Path(template_filename).stem
    script_filename = f"{prefix}{template_filename_stripped}"
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


def collect_package_ssh(
    *,
    task_group_activity_id: int,
    task_group: TaskGroupV2,
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
        task_group_activity_id:
        task_group:
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
        logger.debug("START")
        for key, value in task_group.model_dump().items():
            logger.debug(f"task_group.{key}: {value}")

        # Open a DB session
        with next(get_sync_db()) as db:

            # Check that SSH connection works
            try:
                fractal_ssh.check_connection()
            except Exception as e:
                logger.error("Cannot establish SSH connection.")
                _handle_failure(
                    task_group_activity_id=task_group_activity_id,
                    logger_name=LOGGER_NAME,
                    log_file_path=log_file_path,
                    exception=e,
                    db=db,
                    task_group_id=task_group.id,
                )
                return

            # Check that the (remote) task_group path does not exist
            if fractal_ssh.remote_exists(task_group.path):
                error_msg = f"{task_group.path} already exists."
                logger.error(error_msg)
                _handle_failure(
                    task_group_activity_id=task_group_activity_id,
                    logger_name=LOGGER_NAME,
                    log_file_path=log_file_path,
                    exception=FileExistsError(error_msg),
                    db=db,
                    task_group_id=task_group.id,
                )
                return

            try:
                # Prepare replacements for task-collection scripts
                python_bin = get_python_interpreter_v2(
                    python_version=task_group.python_version
                )
                install_string = task_group.pip_install_string
                settings = Inject(get_settings)
                replacements = [
                    ("__PACKAGE_NAME__", task_group.pkg_name),
                    ("__PACKAGE_ENV_DIR__", task_group.venv_path),
                    ("__PYTHON__", python_bin),
                    ("__INSTALL_STRING__", install_string),
                    (
                        "__FRACTAL_MAX_PIP_VERSION__",
                        settings.FRACTAL_MAX_PIP_VERSION,
                    ),
                    (
                        "__PINNED_PACKAGE_LIST__",
                        task_group.pinned_package_versions_string,
                    ),
                ]
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
                        f"{TaskGroupActivityActionV2.COLLECT}"
                    ),
                    logger_name=LOGGER_NAME,
                    fractal_ssh=fractal_ssh,
                )

                logger.debug("installing - START")

                _set_task_group_activity_status(
                    task_group_activity_id=task_group_activity_id,
                    new_status=TaskGroupActivityStatusV2.ONGOING,
                    logger_name=LOGGER_NAME,
                    db=db,
                )

                _refresh_logs(
                    task_group_activity_id=task_group_activity_id,
                    log_file_path=log_file_path,
                    db=db,
                )

                # Create remote `task_group.path` and `script_dir_remote`
                # folders (note that because of `parents=True` we  are in
                # the `no error if existing, make parent directories as
                # needed` scenario for `mkdir`)
                fractal_ssh.mkdir(folder=task_group.path, parents=True)
                fractal_ssh.mkdir(folder=script_dir_remote, parents=True)

                # Run script 1
                stdout = _customize_and_run_template(
                    template_filename="_1_create_venv.sh",
                    **common_args,
                )
                _refresh_logs(
                    task_group_activity_id=task_group_activity_id,
                    log_file_path=log_file_path,
                    db=db,
                )

                # Run script 2
                stdout = _customize_and_run_template(
                    template_filename="_2_preliminary_pip_operations.sh",
                    **common_args,
                )
                _refresh_logs(
                    task_group_activity_id=task_group_activity_id,
                    log_file_path=log_file_path,
                    db=db,
                )

                # Close db connections before long pip related operations.
                # WARNING: this expunges all ORM objects.
                # https://docs.sqlalchemy.org/en/20/orm/session_api.html#sqlalchemy.orm.Session.close
                db.close()

                # Run script 3
                stdout = _customize_and_run_template(
                    template_filename="_3_pip_install.sh",
                    **common_args,
                )
                _refresh_logs(
                    task_group_activity_id=task_group_activity_id,
                    log_file_path=log_file_path,
                    db=db,
                )

                # Run script 4
                pip_freeze_stdout = _customize_and_run_template(
                    template_filename="_4_pip_freeze.sh",
                    **common_args,
                )
                _refresh_logs(
                    task_group_activity_id=task_group_activity_id,
                    log_file_path=log_file_path,
                    db=db,
                )

                # Run script 5
                stdout = _customize_and_run_template(
                    template_filename="_5_pip_show.sh",
                    **common_args,
                )
                pkg_attrs = parse_script_pip_show(stdout)
                for key, value in pkg_attrs.items():
                    logger.debug(
                        f"collecting - parsed from pip-show: {key}={value}"
                    )
                # Check package_name match between pip show and task-group
                package_name_pip_show = pkg_attrs.get("package_name")
                package_name_task_group = task_group.pkg_name
                compare_package_names(
                    pkg_name_pip_show=package_name_pip_show,
                    pkg_name_task_group=package_name_task_group,
                    logger_name=LOGGER_NAME,
                )

                _refresh_logs(
                    task_group_activity_id=task_group_activity_id,
                    log_file_path=log_file_path,
                    db=db,
                )

                # Extract/drop parsed attributes
                package_name = package_name_task_group
                python_bin = pkg_attrs.pop("python_bin")
                package_root_parent_remote = pkg_attrs.pop(
                    "package_root_parent"
                )
                manifest_path_remote = pkg_attrs.pop("manifest_path")

                # FIXME SSH: Use more robust logic to determine `package_root`.
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
                logger.info(f"collecting - loaded {manifest_path_remote=}")
                pkg_manifest = ManifestV2(**pkg_manifest_dict)
                logger.info("collecting - manifest is a valid ManifestV2")

                logger.info("collecting - _prepare_tasks_metadata - start")
                task_list = _prepare_tasks_metadata(
                    package_manifest=pkg_manifest,
                    package_version=task_group.version,
                    package_root=Path(package_root_remote),
                    python_bin=Path(python_bin),
                )
                logger.info("collecting - _prepare_tasks_metadata - end")

                logger.info(
                    "collecting - create_db_tasks_and_update_task_group - "
                    "start"
                )
                create_db_tasks_and_update_task_group(
                    task_list=task_list,
                    task_group_id=task_group.id,
                    db=db,
                )
                logger.info(
                    "collecting - create_db_tasks_and_update_task_group - end"
                )

                logger.info(
                    "collecting - add pip freeze stdout to TaskGroupV2 - start"
                )

                update_task_group_pip_freeze(
                    task_group_id=task_group.id,
                    pip_freeze_stdout=pip_freeze_stdout,
                    db=db,
                )

                logger.info(
                    "collecting - add pip freeze stdout to TaskGroupV2 - end"
                )

                logger.debug("collecting - END")
                _refresh_logs(
                    task_group_activity_id=task_group_activity_id,
                    log_file_path=log_file_path,
                    db=db,
                )

                # Finalize (write metadata to DB)
                logger.debug("finalising - START")

                _set_task_group_activity_status(
                    task_group_activity_id=task_group_activity_id,
                    new_status=TaskGroupActivityStatusV2.OK,
                    logger_name=LOGGER_NAME,
                    db=db,
                )

                _refresh_logs(
                    task_group_activity_id=task_group_activity_id,
                    log_file_path=log_file_path,
                    db=db,
                )

                logger.debug("finalising - END")
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
                _handle_failure(
                    task_group_activity_id=task_group_activity_id,
                    log_file_path=log_file_path,
                    logger_name=LOGGER_NAME,
                    exception=collection_e,
                    db=db,
                    task_group_id=task_group.id,
                )
    return
