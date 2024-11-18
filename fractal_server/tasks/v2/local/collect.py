import json
import logging
import shutil
import time
from pathlib import Path
from tempfile import TemporaryDirectory

from ..utils_database import create_db_tasks_and_update_task_group
from ._utils import _customize_and_run_template
from fractal_server.app.db import get_sync_db
from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.schemas.v2 import TaskGroupActivityActionV2
from fractal_server.app.schemas.v2 import TaskGroupActivityStatusV2
from fractal_server.app.schemas.v2.manifest import ManifestV2
from fractal_server.logger import get_logger
from fractal_server.logger import set_logger
from fractal_server.tasks.utils import get_log_path
from fractal_server.tasks.v2.local._utils import check_task_files_exist
from fractal_server.tasks.v2.utils_background import _prepare_tasks_metadata
from fractal_server.tasks.v2.utils_background import add_commit_refresh
from fractal_server.tasks.v2.utils_background import fail_and_cleanup
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

LOGGER_NAME = __name__


def _copy_wheel_file_local(task_group: TaskGroupV2) -> str:
    logger = get_logger(LOGGER_NAME)
    source = task_group.wheel_path
    dest = (
        Path(task_group.path) / Path(task_group.wheel_path).name
    ).as_posix()
    logger.debug(f"[_copy_wheel_file] START {source=} {dest=}")
    shutil.copy(task_group.wheel_path, task_group.path)
    logger.debug(f"[_copy_wheel_file] END {source=} {dest=}")
    return dest


def collect_local(
    *,
    task_group_activity_id: int,
    task_group_id: int,
) -> None:
    """
    Collect a task package.

    This function is run as a background task, therefore exceptions must be
    handled.

    NOTE: by making this function sync, it runs within a thread - due to
    starlette/fastapi handling of background tasks (see
    https://github.com/encode/starlette/blob/master/starlette/background.py).


    Arguments:
        task_group_id:
        task_group_activity_id:
    """

    with TemporaryDirectory() as tmpdir:
        log_file_path = get_log_path(Path(tmpdir))
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

            # Check that the (local) task_group path does exist
            if Path(task_group.path).exists():
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

                # Create task_group.path folder
                Path(task_group.path).mkdir(parents=True)
                logger.debug(f"Created {task_group.path}")

                # Copy wheel file into task group path
                if task_group.wheel_path:
                    new_wheel_path = _copy_wheel_file_local(
                        task_group=task_group
                    )
                    task_group.wheel_path = new_wheel_path
                    task_group = add_commit_refresh(obj=task_group, db=db)

                # Prepare replacements for templates
                replacements = get_collection_replacements(
                    task_group=task_group,
                    python_bin=get_python_interpreter_v2(
                        python_version=task_group.python_version
                    ),
                )

                # Prepare common arguments for `_customize_and_run_template``
                common_args = dict(
                    replacements=replacements,
                    script_dir=(
                        Path(task_group.path) / SCRIPTS_SUBFOLDER
                    ).as_posix(),
                    prefix=(
                        f"{int(time.time())}_"
                        f"{TaskGroupActivityActionV2.COLLECT}_"
                    ),
                    logger_name=LOGGER_NAME,
                )

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
                    logger.debug(f"Parsed from pip-show: {key}={value}")
                # Check package_name match between pip show and task-group
                task_group = db.get(TaskGroupV2, task_group_id)
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
                # Examples: use `importlib.util.find_spec`, or parse the
                # output of `pip show --files {package_name}`.
                package_name_underscore = package_name.replace("-", "_")
                package_root = (
                    Path(package_root_parent) / package_name_underscore
                ).as_posix()

                # Read and validate manifest file
                manifest_path = pkg_attrs.pop("manifest_path")
                logger.info(f"now loading {manifest_path=}")
                with open(manifest_path) as json_data:
                    pkg_manifest_dict = json.load(json_data)
                logger.info(f"loaded {manifest_path=}")
                logger.info("now validating manifest content")
                pkg_manifest = ManifestV2(**pkg_manifest_dict)
                logger.info("validated manifest content")
                activity.log = get_current_log(log_file_path)
                activity = add_commit_refresh(obj=activity, db=db)

                logger.info("_prepare_tasks_metadata - start")
                task_list = _prepare_tasks_metadata(
                    package_manifest=pkg_manifest,
                    package_version=task_group.version,
                    package_root=Path(package_root),
                    python_bin=Path(python_bin),
                )
                check_task_files_exist(task_list=task_list)
                logger.info("_prepare_tasks_metadata - end")
                activity.log = get_current_log(log_file_path)
                activity = add_commit_refresh(obj=activity, db=db)

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

                fail_and_cleanup(
                    task_group=task_group,
                    task_group_activity=activity,
                    logger_name=LOGGER_NAME,
                    log_file_path=log_file_path,
                    exception=collection_e,
                    db=db,
                )
        return
