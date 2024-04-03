"""
The main function exported from this module is `background_collect_pip`, which
is used as a background task for the task-collection endpoint.
"""
import json
from pathlib import Path
from shutil import rmtree as shell_rmtree

from ..utils import _init_venv
from ..utils import _normalize_package_name
from ..utils import get_collection_log
from ..utils import get_collection_path
from ..utils import get_log_path
from ..utils import slugify_task_name
from ._TaskCollectPip import _TaskCollectPip
from fractal_server.app.db import DBSyncSession
from fractal_server.app.db import get_sync_db
from fractal_server.app.models import State
from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.schemas.v2 import TaskCollectStatusV2
from fractal_server.app.schemas.v2 import TaskCreateV2
from fractal_server.app.schemas.v2 import TaskReadV2
from fractal_server.logger import close_logger
from fractal_server.logger import get_logger
from fractal_server.logger import set_logger
from fractal_server.utils import execute_command


async def _pip_install(
    venv_path: Path,
    task_pkg: _TaskCollectPip,
    logger_name: str,
) -> Path:
    """
    Install package in venv

    Args:
        venv_path:
        task_pkg:
        logger_name:

    Returns:
        The location of the package.
    """

    logger = get_logger(logger_name)

    pip = venv_path / "venv/bin/pip"

    extras = f"[{task_pkg.package_extras}]" if task_pkg.package_extras else ""

    if task_pkg.is_local_package:
        pip_install_str = f"{task_pkg.package_path.as_posix()}{extras}"
    else:
        version_string = (
            f"=={task_pkg.package_version}" if task_pkg.package_version else ""
        )
        pip_install_str = f"{task_pkg.package}{extras}{version_string}"

    cmd_install = f"{pip} install {pip_install_str}"
    cmd_inspect = f"{pip} show {task_pkg.package}"

    await execute_command(
        cwd=venv_path,
        command=f"{pip} install --upgrade pip",
        logger_name=logger_name,
    )
    await execute_command(
        cwd=venv_path, command=cmd_install, logger_name=logger_name
    )
    if task_pkg.pinned_package_versions:
        for (
            pinned_pkg_name,
            pinned_pkg_version,
        ) in task_pkg.pinned_package_versions.items():

            logger.debug(
                "Specific version required: "
                f"{pinned_pkg_name}=={pinned_pkg_version}"
            )
            logger.debug(
                "Preliminary check: verify that "
                f"{pinned_pkg_version} is already installed"
            )
            stdout_inspect = await execute_command(
                cwd=venv_path,
                command=f"{pip} show {pinned_pkg_name}",
                logger_name=logger_name,
            )
            current_version = next(
                line.split()[-1]
                for line in stdout_inspect.split("\n")
                if line.startswith("Version:")
            )
            if current_version != pinned_pkg_version:
                logger.debug(
                    f"Currently installed version of {pinned_pkg_name} "
                    f"({current_version}) differs from pinned version "
                    f"({pinned_pkg_version}); "
                    f"install version {pinned_pkg_version}."
                )
                await execute_command(
                    cwd=venv_path,
                    command=(
                        f"{pip} install "
                        f"{pinned_pkg_name}=={pinned_pkg_version}"
                    ),
                    logger_name=logger_name,
                )
            else:
                logger.debug(
                    f"Currently installed version of {pinned_pkg_name} "
                    f"({current_version}) already matches the pinned version."
                )

    # Extract package installation path from `pip show`
    stdout_inspect = await execute_command(
        cwd=venv_path, command=cmd_inspect, logger_name=logger_name
    )

    location = Path(
        next(
            line.split()[-1]
            for line in stdout_inspect.split("\n")
            if line.startswith("Location:")
        )
    )

    # NOTE
    # https://packaging.python.org/en/latest/specifications/recording-installed-packages/
    # This directory is named as {name}-{version}.dist-info, with name and
    # version fields corresponding to Core metadata specifications. Both
    # fields must be normalized (see the name normalization specification and
    # the version normalization specification), and replace dash (-)
    # characters with underscore (_) characters, so the .dist-info directory
    # always has exactly one dash (-) character in its stem, separating the
    # name and version fields.
    package_root = location / (task_pkg.package.replace("-", "_"))
    logger.debug(f"[_pip install] {location=}")
    logger.debug(f"[_pip install] {task_pkg.package=}")
    logger.debug(f"[_pip install] {package_root=}")
    if not package_root.exists():
        raise RuntimeError(
            "Could not determine package installation location."
        )
    return package_root


async def _create_venv_install_package(
    *,
    task_pkg: _TaskCollectPip,
    path: Path,
    logger_name: str,
) -> tuple[Path, Path]:
    """Create venv and install package

    Args:
        path: the directory in which to create the environment
        task_pkg: object containing the different metadata required to install
            the package

    Returns:
        python_bin: path to venv's python interpreter
        package_root: the location of the package manifest
    """

    # Normalize package name
    task_pkg.package_name = _normalize_package_name(task_pkg.package_name)
    task_pkg.package = _normalize_package_name(task_pkg.package)

    python_bin = await _init_venv(
        path=path,
        python_version=task_pkg.python_version,
        logger_name=logger_name,
    )
    package_root = await _pip_install(
        venv_path=path, task_pkg=task_pkg, logger_name=logger_name
    )
    return python_bin, package_root


async def create_package_environment_pip(
    *,
    task_pkg: _TaskCollectPip,
    venv_path: Path,
    logger_name: str,
) -> list[TaskCreateV2]:
    """
    Create environment, install package, and prepare task list
    """

    logger = get_logger(logger_name)

    # Normalize package name
    task_pkg.package_name = _normalize_package_name(task_pkg.package_name)
    task_pkg.package = _normalize_package_name(task_pkg.package)

    # Only proceed if package, version and manifest attributes are set
    task_pkg.check()

    try:

        logger.debug("Creating venv and installing package")
        python_bin, package_root = await _create_venv_install_package(
            path=venv_path,
            task_pkg=task_pkg,
            logger_name=logger_name,
        )
        logger.debug("Venv creation and package installation ended correctly.")

        # Prepare task_list with appropriate metadata
        logger.debug("Creating task list from manifest")
        task_list = []
        for t in task_pkg.package_manifest.task_list:
            # Fill in attributes for TaskCreate
            task_attributes = {}
            task_attributes["version"] = task_pkg.package_version
            task_name_slug = slugify_task_name(t.name)
            task_attributes[
                "source"
            ] = f"{task_pkg.package_source}:{task_name_slug}"
            # Executables
            if t.executable_non_parallel is not None:
                non_parallel_path = package_root / t.executable_non_parallel
                if not non_parallel_path.exists():
                    raise FileNotFoundError(
                        f"Cannot find executable `{non_parallel_path}` "
                        f"for task `{t.name}`"
                    )
                task_attributes[
                    "command_non_parallel"
                ] = f"{python_bin.as_posix()} {non_parallel_path.as_posix()}"
            if t.executable_parallel is not None:
                parallel_path = package_root / t.executable_parallel
                if not parallel_path.exists():
                    raise FileNotFoundError(
                        f"Cannot find executable `{parallel_path}` "
                        f"for task `{t.name}`"
                    )
                task_attributes[
                    "command_parallel"
                ] = f"{python_bin.as_posix()} {parallel_path.as_posix()}"

            manifest = task_pkg.package_manifest
            if manifest.has_args_schemas:
                task_attributes[
                    "args_schema_version"
                ] = manifest.args_schema_version

            this_task = TaskCreateV2(
                **t.dict(
                    exclude={"executable_non_parallel", "executable_parallel"}
                ),
                **task_attributes,
            )
            task_list.append(this_task)
        logger.debug("Task list created correctly")
    except Exception as e:
        logger.error("Task manifest loading failed")
        raise e
    return task_list


def _get_task_type(task: TaskCreateV2) -> str:
    if task.command_non_parallel is None:
        return "parallel"
    elif task.command_parallel is None:
        return "non_parallel"
    else:
        return "compound"


async def _insert_tasks(
    task_list: list[TaskCreateV2],
    db: DBSyncSession,
) -> list[TaskV2]:
    """
    Insert tasks into database
    """

    task_db_list = [
        TaskV2(**t.dict(), type=_get_task_type(t)) for t in task_list
    ]
    db.add_all(task_db_list)
    db.commit()
    for t in task_db_list:
        db.refresh(t)
    db.close()
    return task_db_list


async def background_collect_pip(
    state_id: int,
    venv_path: Path,
    task_pkg: _TaskCollectPip,
) -> None:
    """
    Install package and collect tasks

    Install a python package and collect the tasks it provides according to
    the manifest.

    In case of error, copy the log into the state and delete the package
    directory.
    """
    logger_name = task_pkg.package.replace("/", "_")
    logger = set_logger(
        logger_name=logger_name,
        log_file_path=get_log_path(venv_path),
    )
    logger.debug("Start background task collection")
    for key, value in task_pkg.dict(exclude={"package_manifest"}).items():
        logger.debug(f"{key}: {value}")

    with next(get_sync_db()) as db:
        state: State = db.get(State, state_id)
        data = TaskCollectStatusV2(**state.data)
        data.info = None

        try:
            # install
            logger.debug("Task-collection status: installing")
            data.status = "installing"

            state.data = data.sanitised_dict()
            db.merge(state)
            db.commit()
            task_list = await create_package_environment_pip(
                venv_path=venv_path,
                task_pkg=task_pkg,
                logger_name=logger_name,
            )

            # collect
            logger.debug("Task-collection status: collecting")
            data.status = "collecting"
            state.data = data.sanitised_dict()
            db.merge(state)
            db.commit()
            tasks = await _insert_tasks(task_list=task_list, db=db)

            # finalise
            logger.debug("Task-collection status: finalising")
            collection_path = get_collection_path(venv_path)
            data.task_list = [
                TaskReadV2(**task.model_dump()) for task in tasks
            ]
            with collection_path.open("w") as f:
                json.dump(data.sanitised_dict(), f, indent=2)

            # Update DB
            data.status = "OK"
            data.log = get_collection_log(venv_path)
            state.data = data.sanitised_dict()
            db.merge(state)
            db.commit()

            # Write last logs to file
            logger.debug("Task-collection status: OK")
            logger.info("Background task collection completed successfully")
            close_logger(logger)
            db.close()

        except Exception as e:
            # Write last logs to file
            logger.debug("Task-collection status: fail")
            logger.info(f"Background collection failed. Original error: {e}")
            close_logger(logger)

            # Update db
            data.status = "fail"
            data.info = f"Original error: {e}"
            data.log = get_collection_log(venv_path)
            state.data = data.sanitised_dict()
            db.merge(state)
            db.commit()
            db.close()

            # Delete corrupted package dir
            logger.info(f"Now deleting temporary folder {venv_path}")
            shell_rmtree(venv_path)
