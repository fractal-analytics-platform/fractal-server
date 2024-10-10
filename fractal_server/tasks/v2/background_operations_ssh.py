import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory

from sqlalchemy.orm.attributes import flag_modified

from .background_operations import _handle_failure
from .background_operations import _prepare_tasks_metadata
from .background_operations import _set_collection_state_data_status
from .database_operations import create_db_tasks_and_update_task_group
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
from fractal_server.tasks.v2.utils import get_python_interpreter_v2

TEMPLATES_DIR = Path(__file__).parent / "templates"


def _parse_script_5_stdout(stdout: str) -> dict[str, str]:
    searches = [
        ("Python interpreter:", "python_bin"),
        ("Package name:", "package_name"),
        ("Package version:", "package_version"),
        ("Package parent folder:", "package_root_parent_remote"),
        ("Manifest absolute path:", "manifest_path_remote"),
    ]
    stdout_lines = stdout.splitlines()
    attributes = dict()
    for search, attribute_name in searches:
        matching_lines = [_line for _line in stdout_lines if search in _line]
        if len(matching_lines) == 0:
            raise ValueError(f"String '{search}' not found in stdout.")
        elif len(matching_lines) > 1:
            raise ValueError(
                f"String '{search}' found too many times "
                f"({len(matching_lines)})."
            )
        else:
            actual_line = matching_lines[0]
            attribute_value = actual_line.split(search)[-1].strip(" ")
            attributes[attribute_name] = attribute_value
    return attributes


def _customize_and_run_template(
    script_filename: str,
    templates_folder: Path,
    replacements: list[tuple[str, str]],
    tmpdir: str,
    logger_name: str,
    fractal_ssh: FractalSSH,
    tasks_base_dir: str,
) -> str:
    """
    Customize one of the template bash scripts, transfer it to the remote host
    via SFTP and then run it via SSH.

    Args:
        script_filename:
        templates_folder:
        replacements:
        tmpdir:
        logger_name:
        fractal_ssh:
    """
    logger = get_logger(logger_name)
    logger.debug(f"_customize_and_run_template {script_filename} - START")

    # Read template
    template_path = templates_folder / script_filename
    with template_path.open("r") as f:
        script_contents = f.read()
    # Customize template
    for old_new in replacements:
        script_contents = script_contents.replace(old_new[0], old_new[1])
    # Write script locally
    script_path_local = (Path(tmpdir) / script_filename).as_posix()
    with open(script_path_local, "w") as f:
        f.write(script_contents)

    # Transfer script to remote host
    script_path_remote = os.path.join(
        tasks_base_dir,
        f"script_{abs(hash(tmpdir))}{script_filename}",
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

    logger.debug(f"_customize_and_run_template {script_filename} - END")
    return stdout


def background_collect_pip_ssh(
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
                ]

                common_args = dict(
                    templates_folder=TEMPLATES_DIR,
                    replacements=replacements,
                    tmpdir=tmpdir,
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
                # Avoid keeping the db session open as we start some possibly
                # long operations that do not use the db
                db.close()

                # `remove_venv_folder_upon_failure` is set to True only if
                # script 1 goes through, which means that the remote folder
                # `package_env_dir` did not already exist. If this remote
                # folder already existed, then script 1 fails and the boolean
                # flag `remove_venv_folder_upon_failure` remains false.
                remove_venv_folder_upon_failure = False
                stdout = _customize_and_run_template(
                    script_filename="_1_create_venv.sh",
                    **common_args,
                )
                remove_venv_folder_upon_failure = True

                stdout = _customize_and_run_template(
                    script_filename="_2_upgrade_pip.sh",
                    **common_args,
                )
                stdout = _customize_and_run_template(
                    script_filename="_3_pip_install.sh",
                    **common_args,
                )
                stdout_pip_freeze = _customize_and_run_template(
                    script_filename="_4_pip_freeze.sh",
                    **common_args,
                )
                logger.debug("installing - END")

                logger.debug("collecting - START")
                _set_collection_state_data_status(
                    state_id=state_id,
                    new_status=CollectionStatusV2.COLLECTING,
                    logger_name=LOGGER_NAME,
                    db=db,
                )
                # Avoid keeping the db session open as we start some possibly
                # long operations that do not use the db
                db.close()

                stdout = _customize_and_run_template(
                    script_filename="_5_pip_show.sh",
                    **common_args,
                )

                pkg_attrs = _parse_script_5_stdout(stdout)
                for key, value in pkg_attrs.items():
                    logger.debug(
                        f"collecting - parsed from pip-show: {key}={value}"
                    )
                # Check package_name match
                # FIXME SSH: Does this work well for non-canonical names?
                package_name_pip_show = pkg_attrs.get("package_name")
                package_name_task_group = task_group.pkg_name
                if package_name_pip_show != package_name_task_group:
                    error_msg = (
                        f"`package_name` mismatch: "
                        f"{package_name_task_group=} but "
                        f"{package_name_pip_show=}"
                    )
                    logger.error(error_msg)
                    raise ValueError(error_msg)
                # Extract/drop parsed attributes
                package_name = pkg_attrs.pop("package_name")
                python_bin = pkg_attrs.pop("python_bin")
                package_root_parent_remote = pkg_attrs.pop(
                    "package_root_parent_remote"
                )
                manifest_path_remote = pkg_attrs.pop("manifest_path_remote")

                # FIXME SSH: Use more robust logic to determine `package_root`,
                # e.g. as in the custom task-collection endpoint (where we use
                # `importlib.util.find_spec`)
                package_name_underscore = package_name.replace("-", "_")
                package_root_remote = (
                    Path(package_root_parent_remote) / package_name_underscore
                ).as_posix()

                # Read and validate remote manifest file
                with fractal_ssh.sftp().open(manifest_path_remote, "r") as f:
                    pkg_manifest_dict = json.load(f)
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

            except Exception as e:
                # Delete corrupted package dir
                _handle_failure(
                    state_id=state_id,
                    log_file_path=log_file_path,
                    logger_name=LOGGER_NAME,
                    exception=e,
                    db=db,
                    task_group_id=task_group.id,
                )
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
                    except Exception as e:
                        logger.error(
                            f"Removing remote folder failed.\n"
                            f"Original error:\n{str(e)}"
                        )
                    else:
                        logger.info(
                            "Not trying to remove remote folder "
                            f"{task_group.path}."
                        )
                return
