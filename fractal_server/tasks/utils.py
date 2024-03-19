import re
import shutil
import sys
from pathlib import Path
from typing import Optional

from fractal_server.config import get_settings
from fractal_server.syringe import Inject

COLLECTION_FILENAME = "collection.json"
COLLECTION_LOG_FILENAME = "collection.log"


def get_python_interpreter(version: Optional[str] = None) -> str:
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
    if version:
        interpreter = shutil.which(f"python{version}")
        if not interpreter:
            raise ValueError(
                f"Python version {version} not available on host."
            )
    else:
        interpreter = sys.executable

    return interpreter


def slugify_task_name(task_name: str) -> str:
    return task_name.replace(" ", "_").lower()


def get_absolute_venv_path(venv_path: Path) -> Path:
    """
    If a path is not absolute, make it a relative path of FRACTAL_TASKS_DIR.
    """
    if venv_path.is_absolute():
        package_path = venv_path
    else:
        settings = Inject(get_settings)
        package_path = settings.FRACTAL_TASKS_DIR / venv_path
    return package_path


def get_collection_path(base: Path) -> Path:
    return base / COLLECTION_FILENAME


def get_log_path(base: Path) -> Path:
    return base / COLLECTION_LOG_FILENAME


def get_collection_log(venv_path: Path) -> str:
    package_path = get_absolute_venv_path(venv_path)
    log_path = get_log_path(package_path)
    log = log_path.open().read()
    return log


def _normalize_package_name(name: str) -> str:
    """
    Implement PyPa specifications for package-name normalization

    The name should be lowercased with all runs of the characters `.`, `-`,
    or `_` replaced with a single `-` character. This can be implemented in
    Python with the re module.
    (https://packaging.python.org/en/latest/specifications/name-normalization)

    Args:
        name: The non-normalized package name.

    Returns:
        The normalized package name.
    """
    return re.sub(r"[-_.]+", "-", name).lower()
