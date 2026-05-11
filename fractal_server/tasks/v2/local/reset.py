import time
from pathlib import Path
from tempfile import TemporaryDirectory

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import Profile
from fractal_server.app.models import Resource
from fractal_server.app.schemas.v2 import TaskGroupActivityAction
from fractal_server.app.schemas.v2 import TaskGroupActivityStatus
from fractal_server.logger import reset_logger_handlers
from fractal_server.logger import set_logger
from fractal_server.tasks.utils import get_log_path
from fractal_server.tasks.v2.utils_background import add_commit_refresh
from fractal_server.tasks.v2.utils_background import fail_and_cleanup
from fractal_server.tasks.v2.utils_background import get_activity_and_task_group
from fractal_server.tasks.v2.utils_background import get_current_log
from fractal_server.tasks.v2.utils_python_interpreter import (
    get_python_interpreter,
)
from fractal_server.tasks.v2.utils_templates import SCRIPTS_SUBFOLDER
from fractal_server.tasks.v2.utils_templates import get_collection_replacements
from fractal_server.utils import get_timestamp

from ._utils import _customize_and_run_template
from ._utils import rmtree_nofail


def reset_local(
    *,
    task_group_activity_id: int,
    task_group_id: int,
    resource: Resource,
    profile: Profile,
) -> None:
    """
    Re-collect a task package.

    This function runs as a background task, therefore exceptions must be
    handled.

    NOTE:  since this function is sync, it runs within a thread - due to
    starlette/fastapi handling of background tasks (see
    https://github.com/encode/starlette/blob/master/starlette/background.py).


    Args:
        task_group_activity_id:
        task_group_id:
        resource:
        profile:
    """

    LOGGER_NAME = f"{__name__}.ID{task_group_activity_id}"

    with TemporaryDirectory() as tmpdir:
        log_file_path = get_log_path(Path(tmpdir))
        logger = set_logger(
            logger_name=LOGGER_NAME,
            log_file_path=log_file_path,
        )

        logger.info("START")
        with next(get_sync_db()) as db:
            db_objects_ok, task_group, activity = get_activity_and_task_group(
                task_group_activity_id=task_group_activity_id,
                task_group_id=task_group_id,
                db=db,
                logger_name=LOGGER_NAME,
            )
            if not db_objects_ok:
                return

            try:
                # Prepare replacements for templates
                python_bin = get_python_interpreter(
                    python_version=task_group.python_version,
                    resource=resource,
                )
                replacements = get_collection_replacements(
                    task_group=task_group,
                    python_bin=python_bin,
                    resource=resource,
                )

                # Prepare common arguments for `_customize_and_run_template``
                common_args = dict(
                    replacements=replacements,
                    script_dir=(
                        Path(task_group.path) / SCRIPTS_SUBFOLDER
                    ).as_posix(),
                    prefix=(
                        f"{int(time.time())}_{TaskGroupActivityAction.RESET}"
                    ),
                    logger_name=LOGGER_NAME,
                )

                # Set status to ONGOING and refresh logs
                activity.status = TaskGroupActivityStatus.ONGOING
                activity.log = get_current_log(log_file_path)
                activity = add_commit_refresh(obj=activity, db=db)

                # Run script 1
                _customize_and_run_template(
                    template_filename="1_create_venv.sh",
                    **common_args,
                )
                activity.log = get_current_log(log_file_path)
                activity = add_commit_refresh(obj=activity, db=db)

                # Run script 2
                _customize_and_run_template(
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

                # Update task_group data
                logger.info("Update TaskGroupV2 - start")
                task_group.env_info = pip_freeze_stdout
                task_group.active = True
                task_group = add_commit_refresh(obj=task_group, db=db)
                logger.info("Update TaskGroupV2 - end")

                # Finalize (write metadata to DB)
                logger.info("finalising - START")
                activity.status = TaskGroupActivityStatus.OK
                activity.timestamp_ended = get_timestamp()
                activity = add_commit_refresh(obj=activity, db=db)
                logger.info("finalising - END")
                logger.info("END")

                reset_logger_handlers(logger)

            except Exception as collection_e:
                rmtree_nofail(
                    folder_path=task_group.venv_path,
                    logger_name=LOGGER_NAME,
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
