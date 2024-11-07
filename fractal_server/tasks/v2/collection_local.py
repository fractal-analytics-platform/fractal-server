import json
import shutil
import time
from pathlib import Path
from tempfile import TemporaryDirectory

from .database_operations import create_db_tasks_and_update_task_group
from .database_operations import update_task_group_pip_freeze
from fractal_server.app.db import get_sync_db
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.schemas.v2 import TaskGroupActivityActionV2
from fractal_server.app.schemas.v2 import TaskGroupActivityStatusV2
from fractal_server.app.schemas.v2.manifest import ManifestV2
from fractal_server.config import get_settings
from fractal_server.logger import get_logger
from fractal_server.logger import set_logger
from fractal_server.syringe import Inject
from fractal_server.tasks.utils import get_log_path
from fractal_server.tasks.v2.utils_background import _handle_failure
from fractal_server.tasks.v2.utils_background import _prepare_tasks_metadata
from fractal_server.tasks.v2.utils_background import _refresh_logs
from fractal_server.tasks.v2.utils_background import (
    _set_task_group_activity_status,
)
from fractal_server.tasks.v2.utils_background import check_task_files_exist
from fractal_server.tasks.v2.utils_package_names import compare_package_names
from fractal_server.tasks.v2.utils_python_interpreter import (
    get_python_interpreter_v2,
)
from fractal_server.tasks.v2.utils_templates import customize_template
from fractal_server.tasks.v2.utils_templates import parse_script_5_stdout
from fractal_server.tasks.v2.utils_templates import SCRIPTS_SUBFOLDER
from fractal_server.utils import execute_command_sync


def _customize_and_run_template(
    template_filename: str,
    replacements: list[tuple[str, str]],
    script_dir: str,
    logger_name: str,
    prefix: int,
) -> str:
    """
    Customize one of the template bash scripts.

    Args:
        template_filename: Filename of the template file (ends with ".sh").
        replacements: Dictionary of replacements.
        script_dir: Local folder where the script will be placed.
        prefix: Prefix for the script filename.
        logger_name: Logger name
    """
    logger = get_logger(logger_name)
    logger.debug(f"_customize_and_run_template {template_filename} - START")

    # Prepare name and path of script
    if not template_filename.endswith(".sh"):
        raise ValueError(
            f"Invalid {template_filename=} (it must end with '.sh')."
        )
    template_filename_stripped = template_filename[:-3]

    script_filename = f"{prefix}{template_filename_stripped}"
    script_path_local = Path(script_dir) / script_filename

    # Read template
    customize_template(
        template_name=template_filename,
        replacements=replacements,
        script_path=script_path_local,
    )

    cmd = f"bash {script_path_local}"
    logger.debug(f"Now run '{cmd}' ")

    stdout = execute_command_sync(command=cmd, logger_name=logger_name)

    logger.debug(f"Standard output of '{cmd}':\n{stdout}")
    logger.debug(f"_customize_and_run_template {template_filename} - END")

    return stdout


def collect_package_local(
    *,
    task_group_activity_id: int,
    task_group: TaskGroupV2,
) -> None:
    """
    Collect a task package.

    This function is run as a background task, therefore exceptions must be
    handled.

    NOTE: by making this function sync, it runs within a thread - due to
    starlette/fastapi handling of background tasks (see
    https://github.com/encode/starlette/blob/master/starlette/background.py).


    Arguments:
        task_group_activity_id:
        task_group:
    """

    # Create the task_group path
    with TemporaryDirectory() as tmpdir:
        # Setup logger in tmpdir
        LOGGER_NAME = "task_collection_local"
        log_file_path = get_log_path(Path(tmpdir))
        logger = set_logger(
            logger_name=LOGGER_NAME,
            log_file_path=log_file_path,
        )

        # Log some info
        logger.debug("START")
        for key, value in task_group.model_dump().items():
            logger.debug(f"task_group.{key}: {value}")

        # Open a DB session
        with next(get_sync_db()) as db:

            # Check that the (local) task_group path does not exist
            if Path(task_group.path).exists():
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
                common_args = dict(
                    replacements=replacements,
                    logger_name=LOGGER_NAME,
                    script_dir=(
                        Path(task_group.path) / SCRIPTS_SUBFOLDER
                    ).as_posix(),
                    prefix=(
                        f"{int(time.time())}_"
                        f"{TaskGroupActivityActionV2.COLLECT}"
                    ),
                )

                logger.debug("installing - START")
                _set_task_group_activity_status(
                    task_group_activity_id=task_group_activity_id,
                    new_status=TaskGroupActivityStatusV2.ONGOING,
                    logger_name=LOGGER_NAME,
                    db=db,
                )

                # Create task_group.path
                Path(task_group.path).mkdir(parents=True)
                logger.debug(f"Created {task_group.path}")

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
                _refresh_logs(
                    task_group_activity_id=task_group_activity_id,
                    log_file_path=log_file_path,
                    db=db,
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
                # Extract/drop parsed attributes
                package_name = package_name_task_group
                python_bin = pkg_attrs.pop("python_bin")
                package_root_parent = pkg_attrs.pop("package_root_parent")

                # TODO : Use more robust logic to determine `package_root`.
                # Examples: use `importlib.util.find_spec`, or parse the output
                # of `pip show --files {package_name}`.
                package_name_underscore = package_name.replace("-", "_")
                package_root = (
                    Path(package_root_parent) / package_name_underscore
                ).as_posix()

                # Read and validate manifest file
                manifest_path = pkg_attrs.pop("manifest_path")
                logger.info(f"collecting - now loading {manifest_path=}")
                with open(manifest_path) as json_data:
                    pkg_manifest_dict = json.load(json_data)
                logger.info(f"collecting - loaded {manifest_path=}")
                logger.info("collecting - now validating manifest content")
                pkg_manifest = ManifestV2(**pkg_manifest_dict)
                logger.info("collecting - validated manifest content")
                _refresh_logs(
                    task_group_activity_id=task_group_activity_id,
                    log_file_path=log_file_path,
                    db=db,
                )

                logger.info("collecting - _prepare_tasks_metadata - start")
                task_list = _prepare_tasks_metadata(
                    package_manifest=pkg_manifest,
                    package_version=task_group.version,
                    package_root=Path(package_root),
                    python_bin=Path(python_bin),
                )
                check_task_files_exist(task_list=task_list)
                logger.info("collecting - _prepare_tasks_metadata - end")
                _refresh_logs(
                    task_group_activity_id=task_group_activity_id,
                    log_file_path=log_file_path,
                    db=db,
                )

                logger.info(
                    "collecting - create_db_tasks_and_update_task_group - "
                    "start"
                )
                task_group = create_db_tasks_and_update_task_group(
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
                    logger.info(f"Now delete folder {task_group.path}")

                    shutil.rmtree(task_group.path)
                    logger.info(f"Deleted folder {task_group.path}")
                except Exception as rm_e:
                    logger.error(
                        "Removing folder failed.\n"
                        f"Original error:\n{str(rm_e)}"
                    )

                _handle_failure(
                    task_group_activity_id=task_group_activity_id,
                    logger_name=LOGGER_NAME,
                    log_file_path=log_file_path,
                    exception=collection_e,
                    db=db,
                    task_group_id=task_group.id,
                )
    return
