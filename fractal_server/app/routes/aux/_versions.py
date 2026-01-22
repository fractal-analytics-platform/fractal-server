from packaging.version import InvalidVersion
from packaging.version import Version

from fractal_server.app.models.v2.task_group import TaskGroupV2


def _version_sort_key(
    task_group: TaskGroupV2,
) -> tuple[int, Version | str | None]:
    """
    Returns a tuple used as (reverse) ordering key for TaskGroups in
    `get_task_group_list`.
    The TaskGroups with a parsable versions are the first in order,
    sorted according to the sorting rules of packaging.version.Version.
    Next in order we have the TaskGroups with non-null non-parsable versions,
    sorted alphabetically.
    """
    if task_group.version is None:
        return (0, None)
    try:
        return (2, Version(task_group.version))
    except InvalidVersion:
        return (1, task_group.version)
