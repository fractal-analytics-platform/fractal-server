import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import Profile
from fractal_server.app.models import Resource
from fractal_server.app.schemas.v2 import TaskGroupActivityStatusV2
from fractal_server.app.schemas.v2 import TaskGroupV2OriginEnum
from fractal_server.logger import reset_logger_handlers
from fractal_server.logger import set_logger
from fractal_server.tasks.utils import get_log_path
from fractal_server.tasks.v2.utils_background import add_commit_refresh
from fractal_server.tasks.v2.utils_background import fail_and_cleanup
from fractal_server.tasks.v2.utils_background import (
    get_activity_and_task_group,
)
from fractal_server.tasks.v2.utils_background import get_current_log
from fractal_server.utils import get_timestamp


def delete_local(
    *,
    task_group_activity_id: int,
    task_group_id: int,
    resource: Resource,
    profile: Profile,
) -> None:
    LOGGER_NAME = f"{__name__}.ID{task_group_activity_id}"

    with TemporaryDirectory() as tmpdir:
        log_file_path = get_log_path(Path(tmpdir))

        logger = set_logger(
            logger_name=LOGGER_NAME,
            log_file_path=log_file_path,
        )
        logger.debug("START")
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
                activity.status = TaskGroupActivityStatusV2.ONGOING
                activity.log = get_current_log(log_file_path)
                activity = add_commit_refresh(obj=activity, db=db)

                db.delete(task_group)
                db.commit()
                logger.debug("Task group removed from database.")

                if task_group.origin != TaskGroupV2OriginEnum.OTHER:
                    logger.debug(f"Removing {task_group.path=}.")
                    shutil.rmtree(task_group.path)
                    logger.debug(f"{task_group.path=} removed.")

                activity.status = TaskGroupActivityStatusV2.OK
                activity.log = get_current_log(log_file_path)
                activity.timestamp_ended = get_timestamp()
                activity = add_commit_refresh(obj=activity, db=db)

                logger.debug("END")

            except Exception as delete_e:
                fail_and_cleanup(
                    task_group=task_group,
                    task_group_activity=activity,
                    logger_name=LOGGER_NAME,
                    log_file_path=log_file_path,
                    exception=delete_e,
                    db=db,
                )
            finally:
                reset_logger_handlers(logger)
