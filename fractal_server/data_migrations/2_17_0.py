"""
Preliminary checks:
1. Env variables (how should they be set?)
2. All non-relevant users must be marked as non-active in advance.

"""
import json
import logging
import os
import sys

from sqlalchemy.orm import Session
from sqlalchemy.sql.operators import is_
from sqlmodel import delete
from sqlmodel import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import Profile
from fractal_server.app.models import ProjectV2
from fractal_server.app.models import Resource
from fractal_server.app.models import TaskGroupV2
from fractal_server.app.models import UserOAuth
from fractal_server.app.models import UserSettings
from fractal_server.app.schemas.v2.resource import cast_serialize_resource
from fractal_server.config import get_settings


logger = logging.getLogger("fix_db")
logger.setLevel(logging.INFO)


def _verify_env_variables():
    for key in (
        "RESOURCE_JSON_FILE",
        "JOBS_REMOTE_DIRS",
        "TASKS_REMOTE_DIRS",
    ):
        if key not in os.environ.keys():
            raise ValueError(f"Env variable {key} is undefined.")


def _verify_users(db: Session):
    logger.info("START `_verify_users_have_project_dir`")
    logger.info("END `_verify_users_have_project_dir`")


def fix_db_slurm_sudo():
    pass


def fix_db_slurm_ssh():
    logger.info("START execution of fix_db_slurm_ssh function")
    settings = get_settings()
    with next(get_sync_db()) as db:
        # (1) Delete all resources&profile
        db.execute(delete(Profile))
        db.commit()  # needed?
        db.execute(delete(Resource))
        db.commit()  # needed?
        print("Removed all existing resources.")
        print("Removed all existing profiles.")

        # (2) Create first resource
        with open(os.environ["RESOURCE_JSON_FILE"]) as f:
            resource_data = json.load(f)
        valid_resource_data = cast_serialize_resource(resource_data)
        resource = Resource(**valid_resource_data)
        db.add(resource)
        db.commit()
        db.refresh(resource)
        db.expunge(resource)
        resource_id = resource.id

        # (3) Update users
        res = db.execute(
            select(UserOAuth)
            .where(is_(UserOAuth.is_active, True))
            .order_by(UserOAuth.id)
        )
        PROFILE_IDS: dict[str, int] = {}
        for user in res.unique().scalars().all():
            print(f"START updating user {user.email}")
            if user.user_settings_id is None:
                sys.exit(f"{user.email=} has {user.user_settings_id=}.")
            user_settings = db.get(UserSettings, user.user_settings_id)

            # project_dir
            if user_settings.project_dir is None:
                sys.exit(
                    f"User {user.email} has {user_settings.project_dir=}. "
                    "Exit."
                )
            user.project_dir = user_settings.project_dir

            # slurm_accounts
            user.slurm_accounts = user_settings.slurm_accounts or []

            # profile_id
            if user_settings.ssh_username is None:
                sys.exit(
                    f"User {user.email} has {user_settings.ssh_username=}. "
                    "Exit."
                )
            if user_settings.ssh_private_key_path is None:
                sys.exit(
                    f"User {user.email} has "
                    f"{user_settings.ssh_private_key_path=}. "
                    "Exit."
                )
            if user_settings.ssh_host is None:
                sys.exit(
                    f"User {user.email} has {user_settings.ssh_host=}. Exit."
                )

            profile_id = PROFILE_IDS.get(user_settings.ssh_username, None)
            if profile_id is None:
                profile = Profile(
                    resource_id=resource_id,
                    resource_type=settings.FRACTAL_RUNNER_BACKEND,
                    username=user_settings.ssh_username,
                    host=user_settings.ssh_host,
                    ssh_key_path=user_settings.ssh_private_key_path,
                    name=f"{user_settings.ssh_username} profile",
                    jobs_remote_dir="/fake",  # FIXME
                    tasks_remote_dir="/fake",  # FIXME
                )
                db.add(profile)
                db.commit()
                db.refresh(profile)
                db.expunge(profile)
                profile_id = profile.id
                PROFILE_IDS[user_settings.ssh_username] = profile_id
            user.profile_id = profile_id
            db.add(user)
            print(f"END   updating user {user.email}")

        # (4) Update task groups
        res = db.execute(select(TaskGroupV2).order_by(TaskGroupV2.id))
        for taskgroup in res.scalars().all():
            taskgroup.resource_id = resource_id
            db.add(taskgroup)

        # (5) Update projects
        res = db.execute(select(ProjectV2).order_by(ProjectV2.id))
        for project in res.scalars().all():
            project.resource_id = resource_id
            db.add(project)

        # (6) Commit
        db.commit()

    logger.info("END execution of fix_db_slurm_ssh function")


def fix_db_local():
    pass


def fix_db():
    settings = get_settings()
    if settings.FRACTAL_RUNNER_BACKEND == "local":
        fix_db_local()
    elif settings.FRACTAL_RUNNER_BACKEND == "slurm_ssh":
        fix_db_slurm_ssh()
    elif settings.FRACTAL_RUNNER_BACKEND == "slurm_sudo":
        fix_db_slurm_sudo()
    else:
        raise ValueError(f"Invalid {settings.FRACTAL_RUNNER_BACKEND=}.")
