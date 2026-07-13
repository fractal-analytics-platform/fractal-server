import json
import sys
from pathlib import Path

from pydantic import RootModel
from sqlalchemy import select
from sqlalchemy import update

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import Resource
from fractal_server.app.models import TaskGroupV2
from fractal_server.app.models import TaskV2
from fractal_server.app.models import UserGroup
from fractal_server.config import get_email_settings
from fractal_server.send_mail import send_fractal_email_or_log_failure
from fractal_server.syringe import Inject
from fractal_server.types import NonEmptyStr

_ThreeStringsTuple = tuple[
    NonEmptyStr,
    NonEmptyStr,
    NonEmptyStr,
]


class _CoreInfoSet(RootModel):
    """
    Set of `(pkg_name, version, task_name)` tuples.
    """

    root: set[_ThreeStringsTuple]


def _read_set_from_file(path: Path | None) -> set[_ThreeStringsTuple]:
    """
    Read a file (if any) and parse into a set of core-task info items.
    """
    json_data = path.read_text() if path else "[]"
    return _CoreInfoSet.model_validate_json(json_data).root


def _get_final_set(
    *,
    base: Path | None = None,
    additions: Path | None = None,
    removals: Path | None = None,
) -> set[_ThreeStringsTuple]:
    base_set = _read_set_from_file(base)
    additions_set = _read_set_from_file(additions)
    removals_set = _read_set_from_file(removals)
    final_set = (base_set.union(additions_set)).difference(removals_set)
    return final_set


def sync_core_tasks(
    *,
    resources_and_groups: Path,
    base: Path | None = None,
    additions: Path | None = None,
    removals: Path | None = None,
):
    """
    FIXME
    """
    resources_and_groups: list[dict[str, int]] = json.loads(
        resources_and_groups.read_text()
    )
    final_set = _get_final_set(
        base=base,
        additions=additions,
        removals=removals,
    )
    with next(get_sync_db()) as db:
        try:
            # Mark all tasks as not core
            stm = (
                update(TaskV2)
                .where(TaskV2.is_core.is_(True))
                .values(is_core=False)
            )
            db.execute(stm)

            for obj in resources_and_groups:
                resource_id = obj["resource_id"]
                user_group_id = obj["user_group_id"]
                db.get_one(Resource, resource_id)
                db.get_one(UserGroup, user_group_id)
                for pkg_name, version, task_name in final_set:
                    stm = (
                        select(TaskV2)
                        .join(
                            TaskGroupV2, TaskV2.taskgroupv2_id == TaskGroupV2.id
                        )
                        .where(TaskGroupV2.user_group_id == user_group_id)
                        .where(TaskGroupV2.resource_id == resource_id)
                        .where(TaskGroupV2.pkg_name == pkg_name)
                        .where(TaskGroupV2.version == version)
                        .where(TaskV2.name == task_name)
                    )
                    res = db.execute(stm)
                    task = res.scalar_one_or_none()
                    if task is not None:
                        task.is_core = True
                        db.add(task)
                        db.flush()
            db.commit()
        except Exception as exc:
            db.rollback()
            msg = (
                "An exception took place, exit without updating the database."
                f" Original error: {str(exc)}"
            )
            email_settings = Inject(get_email_settings)
            send_fractal_email_or_log_failure(
                subject="Failed core-tasks synchronization",
                msg=msg,
                email_settings=email_settings.public,
            )
            sys.exit(msg)
