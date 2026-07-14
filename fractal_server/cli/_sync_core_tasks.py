import json
import sys
from pathlib import Path

from pydantic import RootModel
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.orm import Session

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import Resource
from fractal_server.app.models import TaskGroupV2
from fractal_server.app.models import TaskV2
from fractal_server.app.models import UserGroup
from fractal_server.config import get_email_settings
from fractal_server.send_mail import send_fractal_email_or_log_failure
from fractal_server.syringe import Inject
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


def sync_core_tasks(
    *,
    resources_and_groups: Path,
    base: Path | None = None,
    add: Path | None = None,
    remove: Path | None = None,
):
    """
    FIXME
    """
    resources_and_groups: list[dict[str, int]] = json.loads(
        resources_and_groups.read_text()
    )
    final_set = _get_final_set(
        base=base,
        add=add,
        remove=remove,
    )
    with next(get_sync_db()) as db:
        count_pre = _count_core_tasks(db)
        print(f"Initial count of core tasks: {count_pre}")

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
        finally:
            count_post = _count_core_tasks(db)
            print(f"Final count of core tasks: {count_post}")
