import logging
import time
from pathlib import Path
from tempfile import TemporaryDirectory

from ..utils_background import add_commit_refresh
from ..utils_background import fail_and_cleanup
from ..utils_templates import get_collection_replacements
from .utils_local import _customize_and_run_template
from fractal_server.app.db import get_sync_db
from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.schemas.v2 import TaskGroupActivityActionV2
from fractal_server.logger import set_logger
from fractal_server.tasks.utils import get_log_path
from fractal_server.tasks.v2.utils_background import get_current_log
from fractal_server.tasks.v2.utils_templates import SCRIPTS_SUBFOLDER


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

            # Check that the (local) task_group path does exist
            if not Path(task_group.venv_path).exists():
                error_msg = f"{task_group} venv_path not exists."
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

            if task_group.pip_freeze is None:

                logging.error(
                    "Cannot find task_group pip_freeze with "
                    f"{task_group_id=} :\n"
                    f"{task_group=}\n. Exit."
                )

                error_msg = f"{task_group=} pip_freeze not exists."
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

            if (
                task_group.origin == "wheel"
                and task_group.wheel_path is None
                and not Path(task_group.wheel_path).exists()
            ):
                logging.error(
                    "Cannot find task_group wheel_path with "
                    f"{task_group_id=} :\n"
                    f"{task_group=}\n. Exit."
                )
                error_msg = f"{task_group} wheel_path not exists."
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

            # Prepare replacements for templates
            replacements = get_collection_replacements(
                task_group=task_group,
                python_bin="/not/applicable",
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
            )
            logger.debug("start - install from pip freeze")
            _customize_and_run_template(
                template_filename="5_pip_install_from_freeze.sh",
                **common_args,
            )

            activity.log = get_current_log(log_file_path)
            task_group.active = True
            task_group = add_commit_refresh(obj=task_group, db=db)
            logger.debug(f"{task_group.active=} - end install from pip freeze")
