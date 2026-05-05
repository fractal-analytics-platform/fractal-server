from pathlib import Path

COLLECTION_FILENAME = "collection.json"
COLLECTION_LOG_FILENAME = "collection.log"
COLLECTION_FREEZE_FILENAME = "collection_freeze.txt"
FORBIDDEN_DEPENDENCY_STRINGS = ["github.com"]

TASK_GROUP_ID_FILENAME = "fractal_task_group_id.txt"


def get_log_path(base: Path) -> Path:
    return base / COLLECTION_LOG_FILENAME
