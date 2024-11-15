import logging
import shutil
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
from fractal_server.logger import set_logger
from fractal_server.tasks.utils import get_log_path
from fractal_server.tasks.v2.utils_background import get_current_log
from fractal_server.tasks.v2.utils_python_interpreter import (
    get_python_interpreter_v2,
)
from fractal_server.tasks.v2.utils_templates import SCRIPTS_SUBFOLDER
from fractal_server.utils import get_timestamp


LOGGER_NAME = __name__


def reactivate_local(
    *,
    task_group_activity_id: int,
    task_group_id: int,
) -> None:
    """
    Reactivate a task group venv.

    This function is run as a background task, therefore exceptions must be
    handled.

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

            # Check that the (local) task_group venv_path does not exist
            if Path(task_group.venv_path).exists():
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
                with open(f"{tmpdir}/pip_freeze.txt", "w") as f:
                    f.write(task_group.pip_freeze)
                replacements.append(
                    ("__PIP_FREEZE_FILE__", f"{tmpdir}/pip_freeze.txt")
                )
                # Prepare common arguments for `_customize_and_run_template``
                common_args = dict(
                    replacements=replacements,
                    script_dir=(
                        Path(task_group.path) / SCRIPTS_SUBFOLDER
                    ).as_posix(),
                    prefix=(
                        f"{int(time.time())}_"
                        f"{TaskGroupActivityActionV2.REACTIVATE}_"
                    ),
                    logger_name=LOGGER_NAME,
                )

                logger.debug("start - create venv")
                _customize_and_run_template(
                    template_filename="1_create_venv.sh",
                    **common_args,
                )
                logger.debug("end - create venv")
                activity.log = get_current_log(log_file_path)
                activity.timestamp_ended = get_timestamp()
                activity = add_commit_refresh(obj=activity, db=db)

                logger.debug("start - install from pip freeze")
                _customize_and_run_template(
                    template_filename="6_pip_install_from_freeze.sh",
                    **common_args,
                )
                logger.debug("end - install from pip freeze")
                activity.log = get_current_log(log_file_path)
                activity.status = TaskGroupActivityStatusV2.OK
                activity.timestamp_ended = get_timestamp()
                activity = add_commit_refresh(obj=activity, db=db)
                task_group.active = True
                task_group = add_commit_refresh(obj=task_group, db=db)
                logger.debug("END")

            except Exception as reactivate_e:
                # Delete corrupted venv_path
                try:
                    logger.info(f"Now delete folder {task_group.venv_path}")
                    shutil.rmtree(task_group.venv_path)
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
        return
