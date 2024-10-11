import re
from pathlib import Path

from fractal_server.config import get_settings
from fractal_server.syringe import Inject

COLLECTION_FILENAME = "collection.json"
COLLECTION_LOG_FILENAME = "collection.log"
COLLECTION_FREEZE_FILENAME = "collection_freeze.txt"


def get_absolute_venv_path_v1(venv_path: Path) -> Path:
    """
    If a path is not absolute, make it a relative path of FRACTAL_TASKS_DIR.

    As of v2.7.0, we rename this to v1 since it is only to be used in v1.
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


def get_freeze_path(base: Path) -> Path:
    return base / COLLECTION_FREEZE_FILENAME


def get_collection_log_v1(path: Path) -> str:
    package_path = get_absolute_venv_path_v1(path)
    log_path = get_log_path(package_path)
    log = log_path.open().read()
    return log


def get_collection_log_v2(path: Path) -> str:
    log_path = get_log_path(path)
    log = log_path.open().read()
    return log


def get_collection_freeze_v1(venv_path: Path) -> str:
    package_path = get_absolute_venv_path_v1(venv_path)
    freeze_path = get_freeze_path(package_path)
    freeze = freeze_path.open().read()
    return freeze


def get_collection_freeze_v2(path: Path) -> str:
    freeze_path = get_freeze_path(path)
    freeze = freeze_path.open().read()
    return freeze


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
