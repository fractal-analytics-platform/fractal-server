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


def get_collection_log_v1(path: Path) -> str:
    package_path = get_absolute_venv_path_v1(path)
    log_path = get_log_path(package_path)
    with log_path.open("r") as f:
        log = f.read()
    return log
