from pathlib import Path
from typing import Literal
from typing import Optional

from fractal_server.config import get_settings
from fractal_server.logger import get_logger
from fractal_server.syringe import Inject
from fractal_server.utils import execute_command


def get_python_interpreter_v2(
    version: Literal["3.9", "3.10", "3.11", "3.12"]
) -> str:
    """
    Return the path to the python interpreter

    Args:
        version: Python version

    Raises:
        ValueError: If the python version requested is not available on the
                    host.

    Returns:
        interpreter: string representing the python executable or its path
    """

    if version not in ["3.9", "3.10", "3.11", "3.12"]:
        raise ValueError(f"Invalid {version=}.")

    settings = Inject(get_settings)
    version_underscore = version.replace(".", "_")
    key = f"FRACTAL_TASKS_PYTHON_{version_underscore}"
    value = getattr(settings, key)
    if value is None:
        raise ValueError(f"Requested {version=}, but {key}={value}.")
    return value


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
    interpreter = get_python_interpreter_v2(version=python_version)
    logger.debug(f"[_init_venv] {interpreter=}")
    await execute_command(
        cwd=path,
        command=f"{interpreter} -m venv venv",
        logger_name=logger_name,
    )
    python_bin = path / "venv/bin/python"
    logger.debug(f"[_init_venv] {python_bin=}")
    return python_bin
