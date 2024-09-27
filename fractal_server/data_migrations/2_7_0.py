import logging

from sqlalchemy import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import TaskGroupV2
from fractal_server.app.models import TaskV2
from fractal_server.app.models import UserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.models import UserSettings
from fractal_server.app.security import FRACTAL_DEFAULT_GROUP_NAME
from fractal_server.data_migrations.tools import _check_current_version

logger = logging.getLogger("fix_db")

DEFAULT_USER_EMAIL = "admin@fractal.xy"


def _get_users_mapping(db) -> dict[str, int]:
    logger.warning("START _check_users")
    print()

    stm_users = select(UserOAuth).order_by(UserOAuth.id)
    users = db.execute(stm_users).scalars().unique().all()
    name_to_user_id = {}
    for user in users:
        logger.warning(f"START handling user {user.id}: '{user.email}'")
        # Compute "name" attribute
        user_settings = db.get(UserSettings, user.user_settings_id)
        name = user.username or user_settings.slurm_user
        logger.warning(f"{name=}")
        # Check for missing values
        if name is None:
            raise ValueError(
                f"User with {user.id=} and {user.email=} has no "
                "`username` or `slurm_user` set."
                "Please fix this issue manually."
            )
        # Check for non-unique values
        existing_user = name_to_user_id.get(name, None)
        if existing_user is not None:
            raise ValueError(
                f"User with {user.id=} and {user.email=} has same "
                f"`(username or slurm_user)={name}` as another user. "
                "Please fix this issue manually."
            )
        # Update dictionary
        name_to_user_id[name] = user.id
        logger.warning(f"END handling user {user.id}: '{user.email}'")
        print()
    logger.warning("END _check_users")
    print()
    return name_to_user_id


def get_default_user_group_id(db):
    stm = select(UserGroup.id).where(
        UserGroup.name == FRACTAL_DEFAULT_GROUP_NAME
    )
    res = db.execute(stm)
    default_group_id = res.scalars().one_or_none()
    if default_group_id is None:
        raise RuntimeError("Default user group is missing.")
    else:
        return default_group_id


def get_default_user_id(db):
    stm = select(UserOAuth.id).where(UserOAuth.email == DEFAULT_USER_EMAIL)
    res = db.execute(stm)
    default_user_id = res.scalars().one_or_none()
    if default_user_id is None:
        raise RuntimeError("Default user is missing.")
    else:
        return default_user_id


def _find_task_associations(db):
    user_mapping = _get_users_mapping(db)
    default_user_id = get_default_user_id(db)
    default_user_group_id = get_default_user_group_id(db)

    stm_tasks = select(TaskV2).order_by(TaskV2.id)
    res = db.execute(stm_tasks).scalars().all()
    task_groups = {}
    for task in res:
        print(task.id, task.source)
        if (
            task.source.startswith(("pip_remote", "pip_local"))
            and task.source.count(":") == 5
        ):
            source_fields = task.source.split(":")
            mode, pkg_name, version, extras, py_version, name = source_fields
            task_group_key = ":".join([pkg_name, version, extras, py_version])
            if task_group_key in task_groups:
                task_groups[task_group_key].append(
                    dict(task=task, user_id=default_user_id)
                )
            else:
                task_groups[task_group_key] = [
                    dict(task=task, user_id=default_user_id)
                ]
        else:
            owner = task.owner
            if owner is None:
                raise RuntimeError(
                    "A Something wrong with "
                    f"{task.id=}, {task.source=}, {task.owner=}"
                )
            user_id = user_mapping.get(owner, None)
            if user_id is None:
                raise RuntimeError(
                    "B Something wrong with "
                    f"{task.id=}, {task.source=}, {task.owner=}"
                )
            task_group_key = "-".join(
                [
                    "NOT_PIP",
                    str(task.id),
                    str(task.version),
                    task.source,
                    str(task.owner),
                ]
            )
            if task_group_key in task_groups:
                raise RuntimeError(
                    "C Something wrong with "
                    f"{task.id=}, {task.source=}, {task.owner=}"
                )
            else:
                task_groups[task_group_key] = [
                    dict(task=task, user_id=user_id)
                ]

    for task_group_key, task_group_objects in task_groups.items():
        print(task_group_key)
        task_group_tasks = [x["task"] for x in task_group_objects]
        task_group_user_ids = [x["user_id"] for x in task_group_objects]
        if len(set(task_group_user_ids)) != 1:
            raise RuntimeError(f"{task_group_user_ids=}")
        for task in task_group_tasks:
            print(f"  {task.source}")
        if not task_group_key.startswith("NOT_PIP"):
            cmd = next(
                getattr(task_group_tasks[0], attr_name)
                for attr_name in ["command_non_parallel", "command_parallel"]
                if getattr(task_group_tasks[0], attr_name) is not None
            )
            python_bin = cmd.split()[0]
        print(f"{python_bin=}")

        # PRINT DRY-RUN VERSION
        task_group = TaskGroupV2(
            user_id=task_group_objects[0]["user_id"],
            user_group_id=default_user_group_id,
            task_list=task_group_tasks,
        )
        db.add(task_group)
        db.commit()
        db.refresh(task_group)
        logger.warning(f"Created task group {task_group.id=}")
        from devtools import debug

        debug(task_group)

        print()

    return


def fix_db(dry_run: bool = False):
    logger.warning("START execution of fix_db function")
    _check_current_version("2.7.0")

    with next(get_sync_db()) as db:
        _get_users_mapping(db)
        _find_task_associations(db)

    logger.warning("END of execution of fix_db function")
