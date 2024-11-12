import logging
import shutil
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
from fractal_server.tasks.v2.utils_templates import SCRIPTS_SUBFOLDER

LOGGER_NAME = __name__


def deactivate_local(
    *,
    task_group_activity_id: int,
    task_group_id: int,
) -> None:
    """
    Deactivate a task group venv.

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
                error_msg = f"{task_group.venv_path} not exists."
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
                        f"{TaskGroupActivityActionV2.DEACTIVATE}_"
                    ),
                )
                pip_freeze_stdout = _customize_and_run_template(
                    template_filename="4_pip_show.sh",
                    **common_args,
                )
                # Update pip-freeze data
                logger.info("Add pip freeze stdout to TaskGroupV2 - start")
                task_group.pip_freeze = pip_freeze_stdout
                task_group = add_commit_refresh(obj=task_group, db=db)
                logger.info("Add pip freeze stdout to TaskGroupV2 - end")

            if task_group.origin == "wheel":
                if (
                    task_group.wheel_path is not None
                    and Path(task_group.wheel_path).exists()
                ):
                    shutil.rmtree(task_group.venv_path)

                else:
                    logging.error(
                        "Cannot find task_group wheel_path with "
                        f"{task_group_id=} :\n"
                        f"{task_group=}\n. Exit."
                    )
                    error_msg = f"{task_group.wheel_path} not exists."
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

            # At this point we are in task_group.origin=="pypi" scenario
            # we are sure that venv_path exists and pip_freeze exists
            shutil.rmtree(task_group.venv_path)
