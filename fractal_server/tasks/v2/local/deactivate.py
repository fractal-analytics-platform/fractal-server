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
from fractal_server.app.schemas.v2 import TaskGroupV2OriginEnum
from fractal_server.app.schemas.v2.task_group import TaskGroupActivityStatusV2
from fractal_server.logger import reset_logger_handlers
from fractal_server.logger import set_logger
from fractal_server.tasks.utils import FORBIDDEN_DEPENDENCY_STRINGS
from fractal_server.tasks.utils import get_log_path
from fractal_server.tasks.v2.utils_background import get_current_log
from fractal_server.tasks.v2.utils_templates import SCRIPTS_SUBFOLDER
from fractal_server.utils import get_timestamp


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

    LOGGER_NAME = f"{__name__}.ID{task_group_activity_id}"

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

            # Check that the (local) task_group venv_path does exist
            if not Path(task_group.venv_path).exists():
                error_msg = f"{task_group.venv_path} does not exist."
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

            try:

                activity.status = TaskGroupActivityStatusV2.ONGOING
                activity = add_commit_refresh(obj=activity, db=db)

                if task_group.pip_freeze is None:
                    logger.warning(
                        "Recreate pip-freeze information, since "
                        f"{task_group.pip_freeze=}. NOTE: this should only "
                        "happen for task groups created before 2.9.0."
                    )
                    # Prepare replacements for templates
                    replacements = get_collection_replacements(
                        task_group=task_group,
                        python_bin="/not/applicable",
                    )

                    # Prepare common arguments for _customize_and_run_template
                    common_args = dict(
                        replacements=replacements,
                        script_dir=(
                            Path(task_group.path) / SCRIPTS_SUBFOLDER
                        ).as_posix(),
                        prefix=(
                            f"{int(time.time())}_"
                            f"{TaskGroupActivityActionV2.DEACTIVATE}_"
                        ),
                        logger_name=LOGGER_NAME,
                    )

                    # Update pip-freeze data
                    pip_freeze_stdout = _customize_and_run_template(
                        template_filename="3_pip_freeze.sh",
                        **common_args,
                    )
                    logger.info("Add pip freeze stdout to TaskGroupV2 - start")
                    activity.log = get_current_log(log_file_path)
                    activity = add_commit_refresh(obj=activity, db=db)
                    task_group.pip_freeze = pip_freeze_stdout
                    task_group = add_commit_refresh(obj=task_group, db=db)
                    logger.info("Add pip freeze stdout to TaskGroupV2 - end")

                # Handle some specific cases for wheel-file case
                if task_group.origin == TaskGroupV2OriginEnum.WHEELFILE:

                    logger.info(
                        f"Handle specific cases for {task_group.origin=}."
                    )

                    # Blocking situation: `wheel_path` is not set or points
                    # to a missing path
                    if (
                        task_group.wheel_path is None
                        or not Path(task_group.wheel_path).exists()
                    ):
                        error_msg = (
                            "Invalid wheel path for task group with "
                            f"{task_group_id=}. {task_group.wheel_path=} is "
                            "unset or does not exist."
                        )
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

                    # Recoverable situation: `wheel_path` was not yet copied
                    # over to the correct server-side folder
                    wheel_path_parent_dir = Path(task_group.wheel_path).parent
                    if wheel_path_parent_dir != Path(task_group.path):
                        logger.warning(
                            f"{wheel_path_parent_dir.as_posix()} differs from "
                            f"{task_group.path}. NOTE: this should only "
                            "happen for task groups created before 2.9.0."
                        )

                        if task_group.wheel_path not in task_group.pip_freeze:
                            raise ValueError(
                                f"Cannot find {task_group.wheel_path=} in "
                                "pip-freeze data. Exit."
                            )

                        logger.info(
                            f"Now copy wheel file into {task_group.path}."
                        )
                        new_wheel_path = (
                            Path(task_group.path)
                            / Path(task_group.wheel_path).name
                        ).as_posix()
                        shutil.copy(task_group.wheel_path, new_wheel_path)
                        logger.info(f"Copied wheel file to {new_wheel_path}.")

                        task_group.wheel_path = new_wheel_path
                        new_pip_freeze = task_group.pip_freeze.replace(
                            task_group.wheel_path,
                            new_wheel_path,
                        )
                        task_group.pip_freeze = new_pip_freeze
                        task_group = add_commit_refresh(obj=task_group, db=db)
                        logger.info(
                            "Updated `wheel_path` and `pip_freeze` "
                            "task-group attributes."
                        )

                # Fail if `pip_freeze` includes "github.com", see
                # https://github.com/fractal-analytics-platform/fractal-server/issues/2142
                for forbidden_string in FORBIDDEN_DEPENDENCY_STRINGS:
                    if forbidden_string in task_group.pip_freeze:
                        raise ValueError(
                            "Deactivation and reactivation of task packages "
                            f"with direct {forbidden_string} dependencies "
                            "are not currently supported. Exit."
                        )

                # We now have all required information for reactivating the
                # virtual environment at a later point

                # Actually mark the task group as non-active
                logger.info("Now setting `active=False`.")
                task_group.active = False
                task_group = add_commit_refresh(obj=task_group, db=db)

                # Proceed with deactivation
                logger.info(f"Now removing {task_group.venv_path}.")
                shutil.rmtree(task_group.venv_path)
                logger.info(f"All good, {task_group.venv_path} removed.")
                activity.status = TaskGroupActivityStatusV2.OK
                activity.log = get_current_log(log_file_path)
                activity.timestamp_ended = get_timestamp()
                activity = add_commit_refresh(obj=activity, db=db)

                reset_logger_handlers(logger)

            except Exception as e:
                fail_and_cleanup(
                    task_group=task_group,
                    task_group_activity=activity,
                    logger_name=LOGGER_NAME,
                    log_file_path=log_file_path,
                    exception=e,
                    db=db,
                )
        return
