from pathlib import Path
from typing import Optional

from ..utils import COLLECTION_FREEZE_FILENAME
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.config import get_settings
from fractal_server.logger import get_logger
from fractal_server.syringe import Inject
from fractal_server.tasks.v2.utils import get_python_interpreter_v2
from fractal_server.utils import execute_command


async def _init_venv_v2(
    *,
    venv_path: Path,
    python_version: Optional[str] = None,
    logger_name: str,
) -> Path:
    """
    Set a virtual environment at `path/venv`

    Args:
        path : Path
            path to the venv actual directory (not its parent).
        python_version : default=None
            Python version the virtual environment will be based upon

    Returns:
        python_bin : Path
            path to python interpreter
    """
    logger = get_logger(logger_name)
    logger.debug(f"[_init_venv_v2] {venv_path=}")
    interpreter = get_python_interpreter_v2(python_version=python_version)
    logger.debug(f"[_init_venv_v2] {interpreter=}")
    await execute_command(
        command=f"{interpreter} -m venv {venv_path}",
        logger_name=logger_name,
    )
    python_bin = venv_path / "bin/python"
    logger.debug(f"[_init_venv_v2] {python_bin=}")
    return python_bin


async def _pip_install(
    task_group: TaskGroupV2,
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
    settings = Inject(get_settings)

    logger = get_logger(logger_name)

    python_bin = Path(task_group.venv_path) / "bin/python"
    pip_install_str = task_group.pip_install_string
    logger.info(f"{pip_install_str=}")

    await execute_command(
        cwd=Path(task_group.venv_path),
        command=(
            f"{python_bin} -m pip install --upgrade "
            f"'pip<={settings.FRACTAL_MAX_PIP_VERSION}'"
        ),
        logger_name=logger_name,
    )
    await execute_command(
        cwd=Path(task_group.venv_path),
        command=f"{python_bin} -m pip install {pip_install_str}",
        logger_name=logger_name,
    )

    if task_group.pinned_package_versions:
        for (
            pinned_pkg_name,
            pinned_pkg_version,
        ) in task_group.pinned_package_versions.items():
            logger.debug(
                "Specific version required: "
                f"{pinned_pkg_name}=={pinned_pkg_version}"
            )
            logger.debug(
                "Preliminary check: verify that "
                f"{pinned_pkg_name} is already installed"
            )
            stdout_show = await execute_command(
                cwd=Path(task_group.venv_path),
                command=f"{python_bin} -m pip show {pinned_pkg_name}",
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
                    cwd=Path(task_group.venv_path),
                    command=(
                        f"{python_bin} -m pip install "
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
        cwd=Path(task_group.venv_path),
        command=f"{python_bin} -m pip show {task_group.pkg_name}",
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
    package_root = location / (task_group.pkg_name.replace("-", "_"))
    logger.debug(f"[_pip install] {location=}")
    logger.debug(f"[_pip install] {task_group.pkg_name=}")
    logger.debug(f"[_pip install] {package_root=}")

    # Run `pip freeze --all` and store its output
    stdout_freeze = await execute_command(
        cwd=Path(task_group.venv_path),
        command=f"{python_bin} -m pip freeze --all",
        logger_name=logger_name,
    )
    with (Path(task_group.path) / COLLECTION_FREEZE_FILENAME).open("w") as f:
        f.write(stdout_freeze)

    return package_root


async def _create_venv_install_package_pip(
    *,
    task_group: TaskGroupV2,
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
        venv_path=Path(task_group.venv_path),
        python_version=task_group.python_version,
        logger_name=logger_name,
    )
    package_root = await _pip_install(
        task_group=task_group,
        logger_name=logger_name,
    )
    return python_bin, package_root
