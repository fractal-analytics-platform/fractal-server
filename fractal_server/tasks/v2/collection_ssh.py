import os
from pathlib import Path
from tempfile import TemporaryDirectory

from sqlalchemy.orm.attributes import flag_modified

from .database_operations import create_db_tasks_and_update_task_group
from .utils_background import _handle_failure
from .utils_background import _prepare_tasks_metadata
from .utils_background import _set_collection_state_data_status
from fractal_server.app.db import get_sync_db
from fractal_server.app.models.v2 import CollectionStateV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.schemas.v2 import CollectionStatusV2
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
from fractal_server.tasks.v2.utils_templates import parse_script_5_stdout


def _customize_and_run_template(
    *,
    template_name: str,
    replacements: list[tuple[str, str]],
    script_dir: str,
    logger_name: str,
    fractal_ssh: FractalSSH,
    tasks_base_dir: str,
) -> str:
    """
    Customize one of the template bash scripts, transfer it to the remote host
    via SFTP and then run it via SSH.

    Args:
        script_filename:
        replacements:
        tmpdir:
        logger_name:
        fractal_ssh:
    """
    logger = get_logger(logger_name)
    logger.debug(f"_customize_and_run_template {template_name} - START")

    script_path_local = Path(script_dir) / template_name

    customize_template(
        template_name=template_name,
        replacements=replacements,
        script_path=script_path_local,
    )

    # Transfer script to remote host
    script_path_remote = os.path.join(
        tasks_base_dir,
        f"script_{abs(hash(script_dir))}{template_name}",
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

    logger.debug(f"_customize_and_run_template {template_name} - END")
    return stdout


def collect_package_ssh(
    *,
    state_id: int,
    task_group: TaskGroupV2,
    fractal_ssh: FractalSSH,
    tasks_base_dir: str,
) -> None:
    """
    Collect a task package over SSH

    This function is run as a background task, therefore exceptions must be
    handled.

    NOTE: by making this function sync, it will run within a thread - due to
    starlette/fastapi handling of background tasks (see
    https://github.com/encode/starlette/blob/master/starlette/background.py).


    Arguments:
        state_id:
        task_group:
        fractal_ssh:
        tasks_base_dir:
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

        # `remove_venv_folder_upon_failure` is set to True only if
        # script 1 goes through, which means that the remote folder
        # `package_env_dir` did not already exist. If this remote
        # folder already existed, then script 1 fails and the boolean
        # flag `remove_venv_folder_upon_failure` remains false.
        remove_venv_folder_upon_failure = False

        # Open a DB session soon, since it is needed for updating `state`
        with next(get_sync_db()) as db:
            try:
                # Prepare replacements for task-collection scripts
                python_bin = get_python_interpreter_v2(
                    python_version=task_group.python_version
                )
                install_string = task_group.pip_install_string
                settings = Inject(get_settings)
                replacements = [
                    ("__PACKAGE_NAME__", task_group.pkg_name),
                    ("__TASK_GROUP_DIR__", task_group.path),
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

                common_args = dict(
                    replacements=replacements,
                    script_dir=tmpdir,
                    logger_name=LOGGER_NAME,
                    fractal_ssh=fractal_ssh,
                    tasks_base_dir=tasks_base_dir,
                )

                fractal_ssh.check_connection()

                logger.debug("installing - START")
                _set_collection_state_data_status(
                    state_id=state_id,
                    new_status=CollectionStatusV2.INSTALLING,
                    logger_name=LOGGER_NAME,
                    db=db,
                )
                _refresh_logs(
                    state_id=state_id,
                    log_file_path=log_file_path,
                    db=db,
                )
                db.close()
                # Create remote folder (note that because of `parents=True` we
                # are in the `no error if existing, make parent directories as
                # needed` scenario)
                fractal_ssh.mkdir(folder=tasks_base_dir, parents=True)

                stdout = _customize_and_run_template(
                    template_name="_1_create_venv.sh",
                    **common_args,
                )
                remove_venv_folder_upon_failure = True
                _refresh_logs(
                    state_id=state_id,
                    log_file_path=log_file_path,
                    db=db,
                )

                stdout = _customize_and_run_template(
                    template_name="_2_preliminary_pip_operations.sh",
                    **common_args,
                )
                _refresh_logs(
                    state_id=state_id,
                    log_file_path=log_file_path,
                    db=db,
                )
                stdout = _customize_and_run_template(
                    template_name="_3_pip_install.sh",
                    **common_args,
                )
                _refresh_logs(
                    state_id=state_id,
                    log_file_path=log_file_path,
                    db=db,
                )
                stdout_pip_freeze = _customize_and_run_template(
                    template_name="_4_pip_freeze.sh",
                    **common_args,
                )
                logger.debug("installing - END")
                _refresh_logs(
                    state_id=state_id,
                    log_file_path=log_file_path,
                    db=db,
                )
                logger.debug("collecting - START")
                _set_collection_state_data_status(
                    state_id=state_id,
                    new_status=CollectionStatusV2.COLLECTING,
                    logger_name=LOGGER_NAME,
                    db=db,
                )
                _refresh_logs(
                    state_id=state_id,
                    log_file_path=log_file_path,
                    db=db,
                )

                stdout = _customize_and_run_template(
                    template_name="_5_pip_show.sh",
                    **common_args,
                )
                pkg_attrs = parse_script_5_stdout(stdout)
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
                    state_id=state_id,
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

                logger.debug("collecting - END")
                _refresh_logs(
                    state_id=state_id,
                    log_file_path=log_file_path,
                    db=db,
                )

                # Finalize (write metadata to DB)
                logger.debug("finalising - START")

                collection_state = db.get(CollectionStateV2, state_id)
                collection_state.data["log"] = log_file_path.open("r").read()
                collection_state.data["freeze"] = stdout_pip_freeze
                collection_state.data["status"] = CollectionStatusV2.OK
                flag_modified(collection_state, "data")
                db.commit()
                logger.debug("finalising - END")
                logger.debug("END")

            except Exception as collection_e:
                # Delete corrupted package dir
                if remove_venv_folder_upon_failure:
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
                else:
                    logger.info(
                        "Not trying to remove remote folder "
                        f"{task_group.path}."
                    )
                _handle_failure(
                    state_id=state_id,
                    log_file_path=log_file_path,
                    logger_name=LOGGER_NAME,
                    exception=collection_e,
                    db=db,
                    task_group_id=task_group.id,
                )
    return
