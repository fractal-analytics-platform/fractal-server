"""
The main function exported from this module is `background_collect_pip`, which
is used as a background task for the task-collection endpoint.
"""
import json
from pathlib import Path
from shutil import rmtree as shell_rmtree
from typing import Optional

from sqlalchemy.orm import Session as DBSyncSession
from sqlalchemy.orm.attributes import flag_modified

from ..utils import get_absolute_venv_path
from ..utils import get_collection_freeze
from ..utils import get_collection_log
from ..utils import get_collection_path
from ..utils import get_log_path
from ..utils import slugify_task_name
from ._TaskCollectPip import _TaskCollectPip
from fractal_server.app.db import get_sync_db
from fractal_server.app.models.v2 import CollectionStateV2
from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.schemas.v2 import CollectionStatusV2
from fractal_server.app.schemas.v2 import TaskCreateV2
from fractal_server.app.schemas.v2 import TaskReadV2
from fractal_server.app.schemas.v2.manifest import ManifestV2
from fractal_server.logger import get_logger
from fractal_server.logger import reset_logger_handlers
from fractal_server.logger import set_logger
from fractal_server.tasks.v2._venv_pip import _create_venv_install_package_pip


def _get_task_type(task: TaskCreateV2) -> str:
    if task.command_non_parallel is None:
        return "parallel"
    elif task.command_parallel is None:
        return "non_parallel"
    else:
        return "compound"


def _insert_tasks(
    task_list: list[TaskCreateV2],
    db: DBSyncSession,
    owner: Optional[str] = None,
) -> list[TaskV2]:
    """
    Insert tasks into database
    """

    owner_dict = dict(owner=owner) if owner is not None else dict()

    task_db_list = [
        TaskV2(**t.dict(), **owner_dict, type=_get_task_type(t))
        for t in task_list
    ]
    db.add_all(task_db_list)
    db.commit()
    for t in task_db_list:
        db.refresh(t)
    db.close()
    return task_db_list


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
    log_file_path: Path,
    logger_name: str,
    exception: Exception,
    db: DBSyncSession,
    venv_path: Optional[Path] = None,
):
    """
    Note: `venv_path` is only required to trigger the folder deletion.
    """

    logger = get_logger(logger_name)
    logger.error(f"Task collection failed. Original error: {str(exception)}")

    _set_collection_state_data_status(
        state_id=state_id,
        new_status=CollectionStatusV2.FAIL,
        logger_name=logger_name,
        db=db,
    )

    new_log = log_file_path.open().read()
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
    # Delete corrupted package dir
    if venv_path is not None:
        logger.info(f"Now delete temporary folder {venv_path}")
        shell_rmtree(venv_path)
        logger.info("Temporary folder deleted")

    reset_logger_handlers(logger)
    return


def _prepare_tasks_metadata(
    *,
    package_manifest: ManifestV2,
    package_source: str,
    python_bin: Path,
    package_root: Path,
    package_version: Optional[str] = None,
) -> list[TaskCreateV2]:
    """
    Based on the package manifest and additional info, prepare the task list.

    Args:
        package_manifest:
        package_source:
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
        task_name_slug = slugify_task_name(_task.name)
        task_attributes["source"] = f"{package_source}:{task_name_slug}"
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
        )
        task_list.append(task_obj)
    return task_list


def _check_task_files_exist(task_list: list[TaskCreateV2]) -> None:
    """
    Check that the modules listed in task commands point to existing files.

    Args: task_list
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


async def background_collect_pip(
    state_id: int,
    venv_path: Path,
    task_pkg: _TaskCollectPip,
) -> None:
    """
    Setup venv, install package, collect tasks.

    This function (executed as background task), includes the several steps
    associated to automated collection of a Python task package.
    1. Preliminary checks
    2. Create venv and run `pip install`
    3. Collect tasks into db
    4. Finalize things.
    5. Handle failures by copying the log into the state and deleting the
       package directory.
    """
    logger_name = task_pkg.package.replace("/", "_")
    logger = set_logger(
        logger_name=logger_name,
        log_file_path=get_log_path(venv_path),
    )

    # Start
    logger.debug("START")
    for key, value in task_pkg.dict(exclude={"package_manifest"}).items():
        logger.debug(f"task_pkg.{key}: {value}")

    with next(get_sync_db()) as db:

        try:
            # Block 1: preliminary checks (only proceed if version and
            # manifest attributes are set).
            # Required: task_pkg
            task_pkg.check()

            # Block 2: create venv and run pip install
            # Required: state_id, venv_path, task_pkg
            logger.debug("installing - START")
            _set_collection_state_data_status(
                state_id=state_id,
                new_status=CollectionStatusV2.INSTALLING,
                logger_name=logger_name,
                db=db,
            )
            python_bin, package_root = await _create_venv_install_package_pip(
                path=venv_path,
                task_pkg=task_pkg,
                logger_name=logger_name,
            )
            logger.debug("installing - END")

            # Block 3: create task metadata and create database entries
            # Required: state_id, python_bin, package_root, task_pkg
            logger.debug("collecting - START")
            _set_collection_state_data_status(
                state_id=state_id,
                new_status=CollectionStatusV2.COLLECTING,
                logger_name=logger_name,
                db=db,
            )
            logger.debug("collecting - prepare tasks and update db " "- START")
            task_list = _prepare_tasks_metadata(
                package_manifest=task_pkg.package_manifest,
                package_version=task_pkg.package_version,
                package_source=task_pkg.package_source,
                package_root=package_root,
                python_bin=python_bin,
            )
            _check_task_files_exist(task_list=task_list)
            tasks = _insert_tasks(task_list=task_list, db=db)
            logger.debug("collecting -  prepare tasks and update db " "- END")
            logger.debug("collecting - END")

            # Block 4: finalize (write collection files, write metadata to DB)
            logger.debug("finalising - START")
            collection_path = get_collection_path(venv_path)
            collection_state = db.get(CollectionStateV2, state_id)
            task_read_list = [
                TaskReadV2(**task.model_dump()).dict() for task in tasks
            ]
            collection_state.data["task_list"] = task_read_list
            collection_state.data["log"] = get_collection_log(venv_path)
            collection_state.data["freeze"] = get_collection_freeze(venv_path)
            with collection_path.open("w") as f:
                json.dump(collection_state.data, f, indent=2)

            flag_modified(collection_state, "data")
            db.commit()
            logger.debug("finalising - END")

        except Exception as e:
            logfile_path = get_log_path(get_absolute_venv_path(venv_path))
            _handle_failure(
                state_id=state_id,
                log_file_path=logfile_path,
                logger_name=logger_name,
                exception=e,
                db=db,
                venv_path=venv_path,
            )
            return

        logger.debug("Task-collection status: OK")
        logger.info("Background task collection completed successfully")
        _set_collection_state_data_status(
            state_id=state_id,
            new_status=CollectionStatusV2.OK,
            logger_name=logger_name,
            db=db,
        )
        reset_logger_handlers(logger)
