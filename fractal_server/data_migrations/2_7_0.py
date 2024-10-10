import logging
import os
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import TaskGroupV2
from fractal_server.app.models import TaskV2
from fractal_server.app.models import UserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.models import UserSettings
from fractal_server.app.security import FRACTAL_DEFAULT_GROUP_NAME
from fractal_server.data_migrations.tools import _check_current_version
from fractal_server.utils import get_timestamp

logger = logging.getLogger("fix_db")


def get_unique_value(list_of_objects: list[dict[str, Any]], key: str):
    """
    Loop over `list_of_objects` and extract (unique) value for `key`.
    """
    unique_values = set()
    for this_obj in list_of_objects:
        this_value = this_obj.get(key, None)
        unique_values.add(this_value)
    if len(unique_values) != 1:
        raise RuntimeError(
            f"There must be a single taskgroup `{key}`, "
            f"but {unique_values=}"
        )
    return unique_values.pop()


def get_users_mapping(db) -> dict[str, int]:
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
        # Fail for missing values
        if name is None:
            raise ValueError(
                f"User with {user.id=} and {user.email=} has no "
                "`username` or `slurm_user` set."
                "Please fix this issue manually."
            )
        # Fail for non-unique values
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

    DEFAULT_USER_EMAIL = os.getenv("FRACTAL_V27_DEFAULT_USER_EMAIL")
    if DEFAULT_USER_EMAIL is None:
        raise ValueError(
            "FRACTAL_V27_DEFAULT_USER_EMAIL env variable is not set. "
            "Please set it to be the email of the user who will own "
            "all previously-global tasks."
        )

    stm = select(UserOAuth.id).where(UserOAuth.email == DEFAULT_USER_EMAIL)
    res = db.execute(stm)
    default_user_id = res.scalars().one_or_none()
    if default_user_id is None:
        raise RuntimeError(
            f"Default user with email {DEFAULT_USER_EMAIL} is missing."
        )
    else:
        return default_user_id


def prepare_task_groups(
    *,
    user_mapping: dict[str, int],
    default_user_group_id: int,
    default_user_id: int,
    db: Session,
):
    stm_tasks = select(TaskV2).order_by(TaskV2.id)
    res = db.execute(stm_tasks).scalars().all()
    task_groups = {}
    for task in res:
        if (
            task.source.startswith(("pip_remote", "pip_local"))
            and task.source.count(":") == 5
        ):
            source_fields = task.source.split(":")
            (
                collection_mode,
                pkg_name,
                version,
                extras,
                python_version,
                name,
            ) = source_fields
            task_group_key = ":".join(
                [pkg_name, version, extras, python_version]
            )
            if collection_mode == "pip_remote":
                origin = "pypi"
            elif collection_mode == "pip_local":
                origin = "wheel-file"
            else:
                raise RuntimeError(
                    f"Invalid {collection_mode=} for {task.source=}."
                )
            new_obj = dict(
                task=task,
                user_id=default_user_id,
                origin=origin,
                pkg_name=pkg_name,
                version=version,
                pip_extras=extras,
                python_version=python_version,
            )

            if task_group_key in task_groups:
                task_groups[task_group_key].append(new_obj)
            else:
                task_groups[task_group_key] = [new_obj]
        else:
            owner = task.owner
            if owner is None:
                raise RuntimeError(
                    "Error: `owner` is `None` for "
                    f"{task.id=}, {task.source=}, {task.owner=}."
                )
            user_id = user_mapping.get(owner, None)
            if user_id is None:
                raise RuntimeError(
                    "Error: `user_id` is `None` for "
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
                    f"ERROR: Duplicated {task_group_key=} for "
                    f"{task.id=}, {task.source=}, {task.owner=}"
                )
            else:
                task_groups[task_group_key] = [
                    dict(
                        task=task,
                        user_id=user_id,
                        origin="other",
                        pkg_name=task.source,
                        version=task.version,
                    )
                ]

    for task_group_key, task_group_objects in task_groups.items():
        print("-" * 80)
        print(f"Start handling task group with key '{task_group_key}")
        task_group_task_list = [item["task"] for item in task_group_objects]
        print("List of tasks to be included")
        for task in task_group_task_list:
            print(f"  {task.id=}, {task.source=}")

        task_group_attributes = dict(
            pkg_name=get_unique_value(task_group_objects, "pkg_name"),
            version=get_unique_value(task_group_objects, "version"),
            origin=get_unique_value(task_group_objects, "origin"),
            user_id=get_unique_value(task_group_objects, "user_id"),
            user_group_id=default_user_group_id,
            python_version=get_unique_value(
                task_group_objects, "python_version"
            ),
            pip_extras=get_unique_value(task_group_objects, "pip_extras"),
            task_list=task_group_task_list,
            active=True,
            timestamp_created=get_timestamp(),
        )

        if not task_group_key.startswith("NOT_PIP"):
            cmd = next(
                getattr(task_group_task_list[0], attr_name)
                for attr_name in ["command_non_parallel", "command_parallel"]
                if getattr(task_group_task_list[0], attr_name) is not None
            )
            python_bin = cmd.split()[0]
            venv_path = Path(python_bin).parents[1].as_posix()
            path = Path(python_bin).parents[2].as_posix()
            task_group_attributes["venv_path"] = venv_path
            task_group_attributes["path"] = path

        print()
        print("List of task-group attributes")
        for key, value in task_group_attributes.items():
            if key != "task_list":
                print(f"  {key}: {value}")

        print()

        task_group = TaskGroupV2(**task_group_attributes)
        db.add(task_group)
        db.commit()
        db.refresh(task_group)
        logger.warning(f"Created task group {task_group.id=}")
        print()

    return


def fix_db():
    logger.warning("START execution of fix_db function")
    _check_current_version("2.7.0")

    with next(get_sync_db()) as db:
        user_mapping = get_users_mapping(db)
        default_user_id = get_default_user_id(db)
        default_user_group_id = get_default_user_group_id(db)

        prepare_task_groups(
            user_mapping=user_mapping,
            default_user_id=default_user_id,
            default_user_group_id=default_user_group_id,
            db=db,
        )

    logger.warning("END of execution of fix_db function")
