from pathlib import Path
from typing import Optional

from fractal_server.app.schemas import TaskCreate
from fractal_server.logger import get_logger
from fractal_server.tasks.utils import _normalize_package_name
from fractal_server.tasks.utils import _TaskCollectPip
from fractal_server.tasks.utils import get_absolute_venv_path
from fractal_server.tasks.utils import get_python_interpreter
from fractal_server.tasks.utils import slugify_task_name
from fractal_server.utils import execute_command


async def _init_venv(
    *,
    path: Path,
    python_version: Optional[str] = None,
    logger_name: str,
) -> Path:
    """
    Set a virtual environment at `path/venv`

    Args:
        path : Path
            path to directory in which to set up the virtual environment
        python_version : default=None
            Python version the virtual environment will be based upon

    Returns:
        python_bin : Path
            path to python interpreter
    """
    logger = get_logger(logger_name)
    logger.debug(f"[_init_venv] {path=}")
    interpreter = get_python_interpreter(version=python_version)
    logger.debug(f"[_init_venv] {interpreter=}")
    await execute_command(
        cwd=path,
        command=f"{interpreter} -m venv venv",
        logger_name=logger_name,
    )
    python_bin = path / "venv/bin/python"
    logger.debug(f"[_init_venv] {python_bin=}")
    return python_bin


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
) -> list[TaskCreate]:
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
            task_executable = package_root / t.executable
            cmd = f"{python_bin.as_posix()} {task_executable.as_posix()}"
            task_name_slug = slugify_task_name(t.name)
            task_source = f"{task_pkg.package_source}:{task_name_slug}"
            if not task_executable.exists():
                raise FileNotFoundError(
                    f"Cannot find executable `{task_executable}` "
                    f"for task `{t.name}`"
                )
            manifest = task_pkg.package_manifest
            if manifest.has_args_schemas:
                additional_attrs = dict(
                    args_schema_version=manifest.args_schema_version
                )
            else:
                additional_attrs = {}
            this_task = TaskCreate(
                **t.dict(),
                command=cmd,
                version=task_pkg.package_version,
                **additional_attrs,
                source=task_source,
            )
            task_list.append(this_task)
        logger.debug("Task list created correctly")
    except Exception as e:
        logger.error("Task manifest loading failed")
        raise e
    return task_list


def get_log_path(base: Path) -> Path:
    return base / "collection.log"


def get_collection_log(venv_path: Path) -> str:
    package_path = get_absolute_venv_path(venv_path)
    log_path = get_log_path(package_path)
    log = log_path.open().read()
    return log
