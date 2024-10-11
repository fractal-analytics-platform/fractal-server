"""
The main function exported from this module is `background_collect_pip`, which
is used as a background task for the task-collection endpoint.
"""
import json
from pathlib import Path
from shutil import rmtree as shell_rmtree
from tempfile import TemporaryDirectory
from typing import Optional
from typing import Union
from zipfile import ZipFile

from sqlalchemy.orm import Session as DBSyncSession
from sqlalchemy.orm.attributes import flag_modified
from sqlmodel import select

from ..utils import get_collection_freeze_v2
from ..utils import get_collection_log_v2
from ..utils import get_collection_path
from ..utils import get_log_path
from .database_operations import create_db_tasks_and_update_task_group
from fractal_server.app.db import get_sync_db
from fractal_server.app.models.v2 import CollectionStateV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.schemas.v2 import CollectionStatusV2
from fractal_server.app.schemas.v2 import TaskCreateV2
from fractal_server.app.schemas.v2 import TaskReadV2
from fractal_server.app.schemas.v2.manifest import ManifestV2
from fractal_server.logger import get_logger
from fractal_server.logger import reset_logger_handlers
from fractal_server.logger import set_logger
from fractal_server.tasks.v2._venv_pip import _create_venv_install_package_pip
from fractal_server.tasks.v2.utils import get_python_interpreter_v2
from fractal_server.utils import execute_command


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
    task_group_id: int,
    path: Optional[Path] = None,
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
    if path is not None and Path(path).exists():
        logger.info(f"Now delete temporary folder {path}")
        shell_rmtree(path)
        logger.info("Temporary folder deleted")

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


async def _download_package(
    *,
    python_version: str,
    pkg_name: str,
    version: str,
    dest: Union[str, Path],
) -> Path:
    """
    Download package to destination and return wheel-file path.
    """
    python_bin = get_python_interpreter_v2(python_version=python_version)
    pip = f"{python_bin} -m pip"
    package_and_version = f"{pkg_name}=={version}"
    cmd = f"{pip} download --no-deps {package_and_version} -d {dest}"
    stdout = await execute_command(command=cmd)
    pkg_file = next(
        line.split()[-1] for line in stdout.split("\n") if "Saved" in line
    )
    return Path(pkg_file)


def _load_manifest_from_wheel(
    wheel_file_path: str,
    logger_name: str,
) -> ManifestV2:
    """
    Given a wheel file on-disk, extract the Fractal manifest.
    """
    logger = get_logger(logger_name)

    with ZipFile(wheel_file_path) as wheel:
        namelist = wheel.namelist()
        try:
            manifest = next(
                name
                for name in namelist
                if "__FRACTAL_MANIFEST__.json" in name
            )
        except StopIteration:
            msg = (
                f"{wheel_file_path} does not include __FRACTAL_MANIFEST__.json"
            )
            logger.error(msg)
            raise ValueError(msg)
        with wheel.open(manifest) as manifest_fd:
            manifest_dict = json.load(manifest_fd)
    manifest_version = str(manifest_dict["manifest_version"])
    if manifest_version != "2":
        msg = f"Manifest version {manifest_version=} not supported"
        logger.error(msg)
        raise ValueError(msg)
    pkg_manifest = ManifestV2(**manifest_dict)
    return pkg_manifest


async def _get_package_manifest(
    *,
    task_group: TaskGroupV2,
    logger_name: str,
) -> ManifestV2:
    wheel_file_path = task_group.wheel_path
    if wheel_file_path is None:
        with TemporaryDirectory() as tmpdir:
            # Copy or download the package wheel file to tmpdir
            wheel_file_path = await _download_package(
                python_version=task_group.python_version,
                pkg_name=task_group.pkg_name,
                version=task_group.version,
                dest=tmpdir,
            )
            wheel_file_path = wheel_file_path.as_posix()
            # Read package manifest from temporary wheel file
            manifest = _load_manifest_from_wheel(
                wheel_file_path=wheel_file_path,
                logger_name=logger_name,
            )
    else:
        # Read package manifest from wheel file
        manifest = _load_manifest_from_wheel(
            wheel_file_path=wheel_file_path,
            logger_name=logger_name,
        )
    return manifest


async def background_collect_pip(
    *,
    state_id: int,
    task_group: TaskGroupV2,
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
    logger_name = (
        f"{task_group.user_id}-{task_group.pkg_name}-{task_group.version}"
    )

    try:
        Path(task_group.path).mkdir(parents=True, exist_ok=False)
    except FileExistsError as e:
        logger = set_logger(
            logger_name=logger_name,
            log_file_path=get_log_path(Path(task_group.path)),
        )

        logfile_path = get_log_path(Path(task_group.path))
        with next(get_sync_db()) as db:
            _handle_failure(
                state_id=state_id,
                log_file_path=logfile_path,
                logger_name=logger_name,
                exception=e,
                db=db,
                path=None,  # Do not remove an existing path
                task_group_id=task_group.id,
            )
            return

    logger = set_logger(
        logger_name=logger_name,
        log_file_path=get_log_path(Path(task_group.path)),
    )

    # Start
    logger.debug("START")
    for key, value in task_group.model_dump().items():
        logger.debug(f"task_group.{key}: {value}")

    with next(get_sync_db()) as db:
        try:
            # Block 1: get and validate manfifest
            pkg_manifest = await _get_package_manifest(
                task_group=task_group,
                logger_name=logger_name,
            )

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
                task_group=task_group,
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
                package_manifest=pkg_manifest,
                package_version=task_group.version,
                package_root=package_root,
                python_bin=python_bin,
            )
            _check_task_files_exist(task_list=task_list)

            # Prepare some task-group attributes
            task_group = create_db_tasks_and_update_task_group(
                task_list=task_list,
                task_group_id=task_group.id,
                db=db,
            )

            logger.debug("collecting -  prepare tasks and update db " "- END")
            logger.debug("collecting - END")

            # Block 4: finalize (write collection files, write metadata to DB)
            logger.debug("finalising - START")
            collection_path = get_collection_path(Path(task_group.path))
            collection_state = db.get(CollectionStateV2, state_id)
            task_read_list = [
                TaskReadV2(**task.model_dump()).dict()
                for task in task_group.task_list
            ]
            collection_state.data["task_list"] = task_read_list
            collection_state.data["log"] = get_collection_log_v2(
                Path(task_group.path)
            )
            collection_state.data["freeze"] = get_collection_freeze_v2(
                Path(task_group.path)
            )
            with collection_path.open("w") as f:
                json.dump(collection_state.data, f, indent=2)

            flag_modified(collection_state, "data")
            db.commit()
            logger.debug("finalising - END")

        except Exception as e:
            logfile_path = get_log_path(Path(task_group.path))
            _handle_failure(
                state_id=state_id,
                log_file_path=logfile_path,
                logger_name=logger_name,
                exception=e,
                db=db,
                path=task_group.path,
                task_group_id=task_group.id,
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
