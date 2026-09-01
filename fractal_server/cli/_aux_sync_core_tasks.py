from pathlib import Path

from pydantic import RootModel
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.orm import Session

from fractal_server.app.models import TaskV2
from fractal_server.types import NonEmptyStr

_SetThreeStringsTuple = set[
    tuple[
        NonEmptyStr,
        NonEmptyStr,
        NonEmptyStr,
    ]
]


class _CoreInfoSet(RootModel):
    """
    Set of `(pkg_name, version, task_name)` tuples.
    """

    root: _SetThreeStringsTuple


def _read_set_from_file(path: Path | None) -> _SetThreeStringsTuple:
    """
    Read a file (if any) and parse into a set of core-task info items.
    """
    json_data = path.read_text() if path else "[]"
    return _CoreInfoSet.model_validate_json(json_data).root


def _get_final_set(
    *,
    base: Path | None = None,
    add: Path | None = None,
    remove: Path | None = None,
) -> _SetThreeStringsTuple:
    base_set = _read_set_from_file(base)
    add_set = _read_set_from_file(add)
    remove_set = _read_set_from_file(remove)
    final_set = (base_set.union(add_set)).difference(remove_set)
    return final_set


def _count_core_tasks(db_sync: Session) -> int:
    """
    Count core tasks.
    """
    count_stm = select(func.count(TaskV2.id)).where(TaskV2.is_core.is_(True))
    res = db_sync.execute(count_stm)
    return res.scalar_one()
