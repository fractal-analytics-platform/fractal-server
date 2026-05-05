from pathlib import Path

COLLECTION_LOG_FILENAME = "collection.log"
FORBIDDEN_DEPENDENCY_STRINGS = ["github.com"]

TASK_GROUP_ID_FILENAME = "fractal_task_group_id.txt"


def get_log_path(base: Path) -> Path:
    return base / COLLECTION_LOG_FILENAME
