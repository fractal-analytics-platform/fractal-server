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


def _check_users(db):
    logger.warning("START _check_users")

    stm_users = select(UserOAuth).order_by(UserOAuth.id)
    users = db.execute(stm_users).scalars().unique().all()
    list_username_or_slurm_user = []
    for user in users:
        logger.warning(f"START handling user {user.id}: '{user.email}'")
        user_settings = db.get(UserSettings, user.user_settings_id)

        # FIXME: this block is not meant to be there, but it's useful to
        # debug next steps
        user.username = f"user_{user.id}"
        db.add(user)
        db.commit()
        db.refresh(user)
        # ------------

        logger.warning(f"{user.username=}, {user_settings.slurm_user=}")
        list_username_or_slurm_user.append(
            user.username or user_settings.slurm_user
        )
        logger.warning(f"END handling user {user.id}: '{user.email}'")
    print(list_username_or_slurm_user)

    if len(list_username_or_slurm_user) != len(
        set(list_username_or_slurm_user)
    ):
        raise ValueError(
            "Non-unique list of usernames or slurm_users. "
            "Manually edit database until this check passes."
        )
    if None in list_username_or_slurm_user:
        raise ValueError(
            "Some user doesn't have either `username` or `slurm_user`."
            "Manually edit database until this check passes."
        )
    logger.warning("END _check_users")
    print()


def _default_user_group_id(db):
    stm = select(UserGroup.id).where(
        UserGroup.name == FRACTAL_DEFAULT_GROUP_NAME
    )
    res = db.execute(stm)
    default_group_id = res.scalars().one_or_none()
    if default_group_id is None:
        raise RuntimeError("Default user group is missing.")
    else:
        return default_group_id


def _create_task_groups_v0(db, dry_run: bool = True):
    stm_tasks = select(TaskV2).order_by(TaskV2.id)
    tasks = db.execute(stm_tasks).scalars().unique().all()
    logger.warning("START _create_task_groups_v0")

    for task in tasks:
        logger.warning(f"START handling task {task.id}: '{task.name}'")
        if task.taskgroupv2_id is not None:
            logger.warning(
                f"This task is already associated to {task.taskgroupv2_id=}, "
                "skip."
            )
        else:
            # FIXME: identify user_id
            task_group = TaskGroupV2(
                user_id=1,
                user_group_id=_default_user_group_id(db),
                task_list=[task],
            )
            db.add(task_group)
            db.commit()
            db.refresh(task_group)
            db.refresh(task)
            logger.warning(f"Created task group {task_group}")
            logger.warning(
                f"This task is now associated to {task.taskgroupv2_id=}."
            )

        logger.warning(f"END   handling task {task.id}: '{task.name}'")
        print()
    logger.warning("END   _create_task_groups_v0")
    print()


def _create_task_groups_v1(db, dry_run: bool = True):
    """
    Finds associations based on source.
    """
    pass


def fix_db():
    logger.warning("START execution of fix_db function")
    _check_current_version("2.7.0")

    with next(get_sync_db()) as db:
        _check_users(db)
        _create_task_groups_v0(db)

    logger.warning("END of execution of fix_db function")
