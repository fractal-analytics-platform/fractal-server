from pathlib import Path
from typing import TypeVar

from sqlalchemy.orm import Session as DBSyncSession

from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.schemas.v2 import TaskCreateV2
from fractal_server.app.schemas.v2 import TaskGroupActivityStatusV2
from fractal_server.app.schemas.v2.manifest import ManifestV2
from fractal_server.app.schemas.v2.task_group import TaskGroupActivityActionV2
from fractal_server.logger import get_logger
from fractal_server.logger import reset_logger_handlers
from fractal_server.utils import get_timestamp

T = TypeVar("T")


def add_commit_refresh(*, obj: T, db: DBSyncSession) -> T:
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


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


def _prepare_tasks_metadata(
    *,
    package_manifest: ManifestV2,
    python_bin: Path,
    package_root: Path,
    package_version: str | None = None,
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
