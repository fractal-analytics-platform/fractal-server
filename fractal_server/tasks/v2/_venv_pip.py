from pathlib import Path
from typing import Optional

from ..utils import COLLECTION_FREEZE_FILENAME
from fractal_server.logger import get_logger
from fractal_server.tasks.v2._TaskCollectPip import _TaskCollectPip
from fractal_server.tasks.v2.utils import get_python_interpreter_v2
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
        pip_install_str = f"{task_pkg.package_name}{extras}{version_string}"

    await execute_command(
        cwd=venv_path,
        command=f"{pip} install --upgrade pip",
        logger_name=logger_name,
    )
    await execute_command(
        cwd=venv_path,
        command=f"{pip} install {pip_install_str}",
        logger_name=logger_name,
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
            stdout_show = await execute_command(
                cwd=venv_path,
                command=f"{pip} show {pinned_pkg_name}",
                logger_name=logger_name,
            )
            current_version = next(
                line.split()[-1]
                for line in stdout_show.split("\n")
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
    stdout_show = await execute_command(
        cwd=venv_path,
        command=f"{pip} show {task_pkg.package_name}",
        logger_name=logger_name,
    )

    location = Path(
        next(
            line.split()[-1]
            for line in stdout_show.split("\n")
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
    package_root = location / (task_pkg.package_name.replace("-", "_"))
    logger.debug(f"[_pip install] {location=}")
    logger.debug(f"[_pip install] {task_pkg.package_name=}")
    logger.debug(f"[_pip install] {package_root=}")

    # Run `pip freeze --all` and store its output
    stdout_freeze = await execute_command(
        cwd=venv_path, command=f"{pip} freeze --all", logger_name=logger_name
    )
    with (venv_path / COLLECTION_FREEZE_FILENAME).open("w") as f:
        f.write(stdout_freeze)

    return package_root


async def _init_venv_v2(
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
    interpreter = get_python_interpreter_v2(python_version=python_version)
    logger.debug(f"[_init_venv] {interpreter=}")
    await execute_command(
        cwd=path,
        command=f"{interpreter} -m venv venv",
        logger_name=logger_name,
    )
    python_bin = path / "venv/bin/python"
    logger.debug(f"[_init_venv] {python_bin=}")
    return python_bin


async def _create_venv_install_package_pip(
    *,
    task_pkg: _TaskCollectPip,
    path: Path,
    logger_name: str,
) -> tuple[Path, Path]:
    """
    Create venv and install package

    Args:
        path: the directory in which to create the environment
        task_pkg: object containing the different metadata required to install
            the package

    Returns:
        python_bin: path to venv's python interpreter
        package_root: the location of the package manifest
    """
    python_bin = await _init_venv_v2(
        path=path,
        python_version=task_pkg.python_version,
        logger_name=logger_name,
    )
    package_root = await _pip_install(
        venv_path=path, task_pkg=task_pkg, logger_name=logger_name
    )
    return python_bin, package_root
