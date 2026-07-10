from pathlib import Path

from pydantic import RootModel

from fractal_server.types import Field
from fractal_server.types import NonEmptyStr

ThreeStringsTuple = tuple[
    NonEmptyStr,
    NonEmptyStr,
    NonEmptyStr,
]


class CoreTasksInfo(RootModel):
    """
    Set of valid items, with unique names.
    """

    root: set[ThreeStringsTuple] = Field(default_factory=list)
    """
    Set of tuples like `(pkg_name, version, task_name)`.
    """


def _read_set_from_file(path: Path | None) -> set[ThreeStringsTuple]:
    if path is None:
        return set()
    else:
        return CoreTasksInfo.model_validate_json(path.read_text()).root


def _get_final_set(
    *,
    public_tasks_path: Path | None = None,
    instance_tasks_path: Path | None = None,
    instance_ignore_path: Path | None = None,
) -> set[ThreeStringsTuple]:
    public_tasks = _read_set_from_file(public_tasks_path)
    instance_tasks = _read_set_from_file(instance_tasks_path)
    instance_ignore = _read_set_from_file(instance_ignore_path)

    final_tasks = (public_tasks.union(instance_tasks)).difference(
        instance_ignore
    )
    return final_tasks


def sync_core_tasks(
    *,
    public_tasks_path: Path | None = None,
    instance_tasks_path: Path | None = None,
    instance_ignore_path: Path | None = None,
):
    final_list = _get_final_set(
        public_tasks_path=public_tasks_path,
        instance_tasks_path=instance_tasks_path,
        instance_ignore_path=instance_ignore_path,
    )
    print(final_list)
