import json
from pathlib import Path

from fractal_server.app.schemas.v1 import TaskCollectStatus
from fractal_server.tasks.utils import get_absolute_venv_path
from fractal_server.tasks.utils import get_collection_path


def get_collection_data(venv_path: Path) -> TaskCollectStatus:
    package_path = get_absolute_venv_path(venv_path)
    collection_path = get_collection_path(package_path)
    with collection_path.open() as f:
        data = json.load(f)
    return TaskCollectStatus(**data)
