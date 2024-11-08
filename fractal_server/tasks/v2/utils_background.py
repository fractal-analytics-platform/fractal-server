from pathlib import Path
from typing import Optional

from sqlalchemy.orm import Session as DBSyncSession
from sqlmodel import select

from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.schemas.v2 import TaskCreateV2
from fractal_server.app.schemas.v2 import TaskGroupActivityStatusV2
from fractal_server.app.schemas.v2.manifest import ManifestV2
from fractal_server.logger import get_logger
from fractal_server.logger import reset_logger_handlers
from fractal_server.utils import get_timestamp


def _set_task_group_activity_status(
    *,
    task_group_activity_id: int,
    new_status: TaskGroupActivityStatusV2,
    logger_name: str,
    db: DBSyncSession,
):
    logger = get_logger(logger_name)
    logger.debug(
        f"{task_group_activity_id=} "
        f"- set task_group_activity.status to {new_status}"
    )
    task_group_activity = db.get(TaskGroupActivityV2, task_group_activity_id)
    task_group_activity.status = TaskGroupActivityStatusV2(new_status)
    if new_status in [
        TaskGroupActivityStatusV2.OK,
        TaskGroupActivityStatusV2.FAILED,
    ]:
        task_group_activity.timestamp_ended = get_timestamp()
    db.add(task_group_activity)
    db.commit()


def _handle_failure(
    task_group_activity_id: int,
    logger_name: str,
    exception: Exception,
    db: DBSyncSession,
    task_group_id: int,
    log_file_path: Path,
):
    logger = get_logger(logger_name)
    logger.error(f"Task collection failed. Original error: {str(exception)}")

    _set_task_group_activity_status(
        task_group_activity_id=task_group_activity_id,
        new_status=TaskGroupActivityStatusV2.FAILED,
        logger_name=logger_name,
        db=db,
    )

    _refresh_logs(
        task_group_activity_id=task_group_activity_id,
        log_file_path=log_file_path,
        db=db,
    )

    logger.info(f"Now delete TaskGroupV2 with {task_group_id=}")

    logger.info("Start of TaskGroupActivityV2 cascade operations.")
    stm = select(TaskGroupActivityV2).where(
        TaskGroupActivityV2.taskgroupv2_id == task_group_id
    )
    res = db.execute(stm)
    task_group_activity_list = res.scalars().all()
    for task_group_activity in task_group_activity_list:
        logger.info(
            f"Setting TaskGroupActivityV2[{task_group_activity.id}]"
            ".taskgroupv2_id to None."
        )
        task_group_activity.taskgroupv2_id = None
        db.add(task_group_activity)
    logger.info("End of TaskGroupActivityV2 cascade operations.")

    task_group = db.get(TaskGroupV2, task_group_id)
    db.delete(task_group)
    db.commit()
    logger.info(f"TaskGroupV2 with {task_group_id=} deleted")

    reset_logger_handlers(logger)
    return


def _prepare_tasks_metadata(
    *,
    package_manifest: ManifestV2,
    python_bin: Path,
    package_root: Path,
    package_version: Optional[str] = None,
) -> list[TaskCreateV2]:
    """
    Based on the package manifest and additional info, prepare the task list.

    Args:
        package_manifest:
        python_bin:
        package_root:
        package_version:
    """
    task_list = []
    for _task in package_manifest.task_list:
        # Set non-command attributes
        task_attributes = {}
        if package_version is not None:
            task_attributes["version"] = package_version
        if package_manifest.has_args_schemas:
            task_attributes[
                "args_schema_version"
            ] = package_manifest.args_schema_version
        # Set command attributes
        if _task.executable_non_parallel is not None:
            non_parallel_path = package_root / _task.executable_non_parallel
            task_attributes["command_non_parallel"] = (
                f"{python_bin.as_posix()} " f"{non_parallel_path.as_posix()}"
            )
        if _task.executable_parallel is not None:
            parallel_path = package_root / _task.executable_parallel
            task_attributes[
                "command_parallel"
            ] = f"{python_bin.as_posix()} {parallel_path.as_posix()}"
        # Create object
        task_obj = TaskCreateV2(
            **_task.dict(
                exclude={
                    "executable_non_parallel",
                    "executable_parallel",
                }
            ),
            **task_attributes,
            authors=package_manifest.authors,
        )
        task_list.append(task_obj)
    return task_list


def check_task_files_exist(task_list: list[TaskCreateV2]) -> None:
    """
    Check that the modules listed in task commands point to existing files.

    Args:
        task_list:
    """
    for _task in task_list:
        if _task.command_non_parallel is not None:
            _task_path = _task.command_non_parallel.split()[1]
            if not Path(_task_path).exists():
                raise FileNotFoundError(
                    f"Task `{_task.name}` has `command_non_parallel` "
                    f"pointing to missing file `{_task_path}`."
                )
        if _task.command_parallel is not None:
            _task_path = _task.command_parallel.split()[1]
            if not Path(_task_path).exists():
                raise FileNotFoundError(
                    f"Task `{_task.name}` has `command_parallel` "
                    f"pointing to missing file `{_task_path}`."
                )


def _refresh_logs(
    *,
    task_group_activity_id: int,
    log_file_path: Path,
    db: DBSyncSession,
) -> None:
    """
    Read logs from file and update them in the db.
    """
    task_group_activity = db.get(TaskGroupActivityV2, task_group_activity_id)
    task_group_activity.log = log_file_path.open("r").read()
    db.add(task_group_activity)
    db.commit()
