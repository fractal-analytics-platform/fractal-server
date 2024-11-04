from pathlib import Path
from typing import Optional

from sqlalchemy.orm import Session as DBSyncSession
from sqlalchemy.orm.attributes import flag_modified
from sqlmodel import select

from fractal_server.app.models.v2 import CollectionStateV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.schemas.v2 import CollectionStatusV2
from fractal_server.app.schemas.v2 import TaskCreateV2
from fractal_server.app.schemas.v2.manifest import ManifestV2
from fractal_server.logger import get_logger
from fractal_server.logger import reset_logger_handlers


def _set_collection_state_data_status(
    *,
    state_id: int,
    new_status: CollectionStatusV2,
    logger_name: str,
    db: DBSyncSession,
):
    logger = get_logger(logger_name)
    logger.debug(f"{state_id=} - set state.data['status'] to {new_status}")
    collection_state = db.get(CollectionStateV2, state_id)
    collection_state.data["status"] = CollectionStatusV2(new_status)
    flag_modified(collection_state, "data")
    db.commit()


def _set_collection_state_data_log(
    *,
    state_id: int,
    new_log: str,
    logger_name: str,
    db: DBSyncSession,
):
    logger = get_logger(logger_name)
    logger.debug(f"{state_id=} - set state.data['log']")
    collection_state = db.get(CollectionStateV2, state_id)
    collection_state.data["log"] = new_log
    flag_modified(collection_state, "data")
    db.commit()


def _set_collection_state_data_info(
    *,
    state_id: int,
    new_info: str,
    logger_name: str,
    db: DBSyncSession,
):
    logger = get_logger(logger_name)
    logger.debug(f"{state_id=} - set state.data['info']")
    collection_state = db.get(CollectionStateV2, state_id)
    collection_state.data["info"] = new_info
    flag_modified(collection_state, "data")
    db.commit()


def _handle_failure(
    state_id: int,
    logger_name: str,
    exception: Exception,
    db: DBSyncSession,
    task_group_id: int,
    log_file_path: Path,
):
    logger = get_logger(logger_name)
    logger.error(f"Task collection failed. Original error: {str(exception)}")

    _set_collection_state_data_status(
        state_id=state_id,
        new_status=CollectionStatusV2.FAIL,
        logger_name=logger_name,
        db=db,
    )

    new_log = log_file_path.open("r").read()

    _set_collection_state_data_log(
        state_id=state_id,
        new_log=new_log,
        logger_name=logger_name,
        db=db,
    )
    # For backwards-compatibility, we also set state.data["info"]
    _set_collection_state_data_info(
        state_id=state_id,
        new_info=f"Original error: {exception}",
        logger_name=logger_name,
        db=db,
    )

    # Delete TaskGroupV2 object / and apply cascade operation to FKs
    logger.info(f"Now delete TaskGroupV2 with {task_group_id=}")
    logger.info("Start of CollectionStateV2 cascade operations.")
    stm = select(CollectionStateV2).where(
        CollectionStateV2.taskgroupv2_id == task_group_id
    )
    res = db.execute(stm)
    collection_states = res.scalars().all()
    for collection_state in collection_states:
        logger.info(
            f"Setting CollectionStateV2[{collection_state.id}].taskgroupv2_id "
            "to None."
        )
        collection_state.taskgroupv2_id = None
        db.add(collection_state)
    logger.info("End of CollectionStateV2 cascade operations.")
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
    state_id: int,
    log_file_path: Path,
    db: DBSyncSession,
) -> None:
    """
    Read logs from file and update them in the db.
    """
    collection_state = db.get(CollectionStateV2, state_id)
    collection_state.data["log"] = log_file_path.open("r").read()
    flag_modified(collection_state, "data")
    db.commit()
