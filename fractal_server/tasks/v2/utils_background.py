import logging
from pathlib import Path
from typing import TypeVar

from sqlalchemy.orm import Session as DBSyncSession

from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.schemas.v2 import TaskCreateV2
from fractal_server.app.schemas.v2 import TaskGroupActivityStatusV2
from fractal_server.app.schemas.v2.manifest import ManifestV2
from fractal_server.app.schemas.v2.task_group import TaskGroupActivityActionV2
from fractal_server.exceptions import UnreachableBranchError
from fractal_server.logger import get_logger
from fractal_server.logger import reset_logger_handlers
from fractal_server.utils import get_timestamp

T = TypeVar("T")


def add_commit_refresh(*, obj: T, db: DBSyncSession) -> T:
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


def get_activity_and_task_group(
    *,
    task_group_activity_id: int,
    task_group_id: int,
    db: DBSyncSession,
    logger_name: str,
) -> tuple[bool, TaskGroupV2, TaskGroupActivityV2]:
    task_group = db.get(TaskGroupV2, task_group_id)
    activity = db.get(TaskGroupActivityV2, task_group_activity_id)
    if activity is None or task_group is None:
        logging.error(
            "Cannot find database rows with "
            f"{task_group_id=} and {task_group_activity_id=}:\n"
            f"{task_group=}\n{activity=}. Exit."
        )
        return False, None, None

    # Log some info about task group
    logger = get_logger(logger_name=logger_name)
    for key, value in task_group.model_dump(exclude={"env_info"}).items():
        logger.debug(f"task_group.{key}: {value}")

    return True, task_group, activity


def fail_and_cleanup(
    task_group: TaskGroupV2,
    task_group_activity: TaskGroupActivityV2,
    logger_name: str,
    exception: Exception,
    log_file_path: Path,
    db: DBSyncSession,
):
    logger = get_logger(logger_name)
    logger.warning(
        f"Task {task_group_activity.action} failed. "
        f"Original error: {str(exception)}"
    )

    task_group_activity.status = TaskGroupActivityStatusV2.FAILED
    task_group_activity.timestamp_ended = get_timestamp()
    task_group_activity.log = get_current_log(log_file_path)
    task_group_activity = add_commit_refresh(obj=task_group_activity, db=db)
    if task_group_activity.action == TaskGroupActivityActionV2.COLLECT:
        db.delete(task_group)
    db.commit()
    reset_logger_handlers(logger)


def prepare_tasks_metadata(
    *,
    package_manifest: ManifestV2,
    package_root: Path,
    python_bin: Path | None = None,
    project_python_wrapper: Path | None = None,
    package_version: str | None = None,
) -> list[TaskCreateV2]:
    """
    Based on the package manifest and additional info, prepare the task list.

    Args:
        package_manifest:
        package_root:
        package_version:
        python_bin:
        project_python_wrapper:
    """

    if bool(project_python_wrapper is None) == bool(python_bin is None):
        raise UnreachableBranchError(
            f"Either {project_python_wrapper} or {python_bin} must be set."
        )

    if python_bin is not None:
        actual_python = python_bin
    else:
        actual_python = project_python_wrapper

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
            cmd_non_parallel = (
                f"{actual_python.as_posix()} {non_parallel_path.as_posix()}"
            )
            task_attributes["command_non_parallel"] = cmd_non_parallel
        if _task.executable_parallel is not None:
            parallel_path = package_root / _task.executable_parallel
            cmd_parallel = (
                f"{actual_python.as_posix()} {parallel_path.as_posix()}"
            )
            task_attributes["command_parallel"] = cmd_parallel
        # Create object
        task_obj = TaskCreateV2(
            **_task.model_dump(
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


def get_current_log(logger_file_path: str) -> str:
    with open(logger_file_path) as f:
        return f.read()
