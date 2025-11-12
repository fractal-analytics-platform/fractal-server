import json
import logging
import sys
from pathlib import Path
from typing import Any

from dotenv.main import DotEnv
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy.sql.operators import is_
from sqlalchemy.sql.operators import is_not
from sqlmodel import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import Profile
from fractal_server.app.models import ProjectV2
from fractal_server.app.models import Resource
from fractal_server.app.models import TaskGroupV2
from fractal_server.app.models import UserOAuth
from fractal_server.app.models import UserSettings
from fractal_server.app.schemas.v2.profile import cast_serialize_profile
from fractal_server.app.schemas.v2.resource import cast_serialize_resource
from fractal_server.config import get_settings
from fractal_server.runner.config import JobRunnerConfigLocal
from fractal_server.runner.config import JobRunnerConfigSLURM
from fractal_server.tasks.config import TasksPixiSettings
from fractal_server.tasks.config import TasksPythonSettings
from fractal_server.types import AbsolutePathStr
from fractal_server.types import ListUniqueNonEmptyString
from fractal_server.urls import normalize_url

logging.basicConfig(level=logging.INFO)


class UserUpdateInfo(BaseModel):
    user_id: int
    project_dir: AbsolutePathStr
    slurm_accounts: ListUniqueNonEmptyString


class ProfileUsersUpdateInfo(BaseModel):
    data: dict[str, Any]
    user_updates: list[UserUpdateInfo]


def _get_user_settings(user: UserOAuth, db: Session) -> UserSettings:
    if user.user_settings_id is None:
        sys.exit(f"User {user.email} is active but {user.user_settings_id=}.")
    user_settings = db.get(UserSettings, user.user_settings_id)
    return user_settings


def assert_user_setting_key(
    user: UserOAuth,
    user_settings: UserSettings,
    keys: list[str],
) -> None:
    for key in keys:
        if getattr(user_settings, key) is None:
            sys.exit(
                f"User {user.email} is active and verified but their "
                f"user settings have {key}=None."
            )


def prepare_profile_and_user_updates() -> dict[str, ProfileUsersUpdateInfo]:
    settings = get_settings()
    profiles_and_users: dict[str, ProfileUsersUpdateInfo] = {}
    with next(get_sync_db()) as db:
        # Get active&verified users
        res = db.execute(
            select(UserOAuth)
            .where(is_(UserOAuth.is_active, True))
            .where(is_(UserOAuth.is_verified, True))
            .order_by(UserOAuth.id)
        )
        for user in res.unique().scalars().all():
            # Get user settings
            user_settings = _get_user_settings(user=user, db=db)
            assert_user_setting_key(user, user_settings, ["project_dir"])

            # Prepare profile data and user update
            new_profile_data = dict()
            if settings.FRACTAL_RUNNER_BACKEND == "local":
                username = None
            if settings.FRACTAL_RUNNER_BACKEND == "slurm_sudo":
                assert_user_setting_key(user, user_settings, ["slurm_user"])
                username = user_settings.slurm_user
            elif settings.FRACTAL_RUNNER_BACKEND == "slurm_ssh":
                assert_user_setting_key(
                    user,
                    user_settings,
                    [
                        "ssh_username",
                        "ssh_private_key_path",
                        "ssh_tasks_dir",
                        "ssh_jobs_dir",
                    ],
                )
                username = user_settings.ssh_username
                new_profile_data.update(
                    ssh_key_path=user_settings.ssh_private_key_path,
                    tasks_remote_dir=normalize_url(
                        user_settings.ssh_tasks_dir
                    ),
                    jobs_remote_dir=normalize_url(user_settings.ssh_jobs_dir),
                )

            new_profile_data.update(
                name=f"Profile {username}",
                username=username,
                resource_type=settings.FRACTAL_RUNNER_BACKEND,
            )
            cast_serialize_profile(new_profile_data)

            user_update_info = UserUpdateInfo(
                user_id=user.id,
                project_dir=normalize_url(user_settings.project_dir),
                slurm_accounts=user_settings.slurm_accounts or [],
            )

            if username in profiles_and_users.keys():
                if profiles_and_users[username].data != new_profile_data:
                    error_msg = (
                        "Profile data mismatch.\n"
                        f"{profiles_and_users[username].data=}\n"
                        f"{new_profile_data=}"
                    )
                    logging.error(error_msg)
                    sys.exit(error_msg)
                profiles_and_users[username].user_updates.append(
                    user_update_info
                )
            else:
                profiles_and_users[username] = ProfileUsersUpdateInfo(
                    data=new_profile_data,
                    user_updates=[user_update_info],
                )

    return profiles_and_users


def get_old_dotenv_variables() -> dict[str, str | None]:
    """
    See
    https://github.com/fractal-analytics-platform/fractal-server/blob/2.16.x/fractal_server/config.py
    """
    OLD_DOTENV_FILE = ".fractal_server.env.old"
    return dict(
        **DotEnv(
            dotenv_path=OLD_DOTENV_FILE,
            override=False,
        ).dict()
    )


def get_TasksPythonSettings(
    old_config: dict[str, str | None]
) -> dict[str, Any]:
    versions = {}
    for version_underscore in ["3_9", "3_10", "3_11", "3_12"]:
        key = f"FRACTAL_TASKS_PYTHON_{version_underscore}"
        version_dot = version_underscore.replace("_", ".")
        value = old_config.get(key, None)
        if value is not None:
            versions[version_dot] = value
    obj = TasksPythonSettings(
        default_version=old_config["FRACTAL_TASKS_PYTHON_DEFAULT_VERSION"],
        versions=versions,
        pip_cache_dir=old_config.get("FRACTAL_PIP_CACHE_DIR", None),
    )
    return obj.model_dump()


def get_TasksPixiSettings(old_config: dict[str, str | None]) -> dict[str, Any]:
    pixi_file = old_config.get("FRACTAL_PIXI_CONFIG_FILE", None)
    if pixi_file is None:
        return {}
    with open(pixi_file) as f:
        old_pixi_config = json.load(f)
    TasksPixiSettings(**old_pixi_config)
    return old_pixi_config


def get_JobRunnerConfigSLURM(
    old_config: dict[str, str | None]
) -> dict[str, Any]:
    slurm_file = old_config["FRACTAL_SLURM_CONFIG_FILE"]
    with open(slurm_file) as f:
        old_slurm_config = json.load(f)
    JobRunnerConfigSLURM(**old_slurm_config)
    return old_slurm_config


def get_JobRunnerConfigLocal(
    old_config: dict[str, str | None]
) -> dict[str, Any]:
    local_file = old_config.get("FRACTAL_LOCAL_CONFIG_FILE", None)
    if local_file is None or not Path(local_file).exists():
        return JobRunnerConfigLocal().model_dump()
    else:
        with open(local_file) as f:
            old_local_config = json.load(f)
        JobRunnerConfigLocal(**old_local_config)
        return old_local_config


def get_ssh_host() -> str:
    with next(get_sync_db()) as db:
        res = db.execute(
            select(UserSettings.ssh_host).where(
                is_not(UserSettings.ssh_host, None)
            )
        )
        hosts = res.scalars().all()
    if len(set(hosts)) > 1:
        host = max(set(hosts), key=hosts.count)
        print(f"MOST FREQUENT HOST: {host}")
    else:
        host = hosts[0]
    return host


def prepare_resource_data(old_config: dict[str, str | None]) -> dict[str, Any]:
    settings = get_settings()

    resource_data = dict(
        type=settings.FRACTAL_RUNNER_BACKEND,
        name="Resource Name",
        tasks_python_config=get_TasksPythonSettings(old_config),
        tasks_pixi_config=get_TasksPixiSettings(old_config),
        tasks_local_dir=old_config["FRACTAL_TASKS_DIR"],
        jobs_local_dir=old_config["FRACTAL_RUNNER_WORKING_BASE_DIR"],
        jobs_poll_interval=int(
            old_config.get("FRACTAL_SLURM_POLL_INTERVAL", 15)
        ),
    )
    if settings.FRACTAL_RUNNER_BACKEND == "local":
        resource_data["jobs_runner_config"] = get_JobRunnerConfigLocal(
            old_config
        )
    elif settings.FRACTAL_RUNNER_BACKEND == "slurm_sudo":
        resource_data["jobs_slurm_python_worker"] = old_config[
            "FRACTAL_SLURM_WORKER_PYTHON"
        ]
        resource_data["jobs_runner_config"] = get_JobRunnerConfigSLURM(
            old_config
        )
    else:
        resource_data["jobs_slurm_python_worker"] = old_config[
            "FRACTAL_SLURM_WORKER_PYTHON"
        ]
        resource_data["jobs_runner_config"] = get_JobRunnerConfigSLURM(
            old_config
        )
        resource_data["host"] = get_ssh_host()

    resource_data = cast_serialize_resource(resource_data)

    return resource_data


def fix_db():
    logging.info("START preliminary checks.")

    # Read old env file
    old_config = get_old_dotenv_variables()

    # Prepare resource data
    logging.info("START prepare_resource_data")
    resource_data = prepare_resource_data(old_config)
    logging.info("END prepare_resource_data")

    # Prepare profile/users data
    logging.info("START prepare_profile_and_user_updates")
    profile_and_user_updates = prepare_profile_and_user_updates()
    logging.info("END prepare_profile_and_user_updates")

    logging.info("END preliminary checks.")
    print()

    with next(get_sync_db()) as db:
        # Create new resource
        resource = Resource(**resource_data)
        db.add(resource)
        db.commit()
        db.refresh(resource)
        db.expunge(resource)
        resource_id = resource.id
        logging.info(f"Created resource with {resource_id=}.")

        # Update task groups
        res = db.execute(select(TaskGroupV2).order_by(TaskGroupV2.id))
        for taskgroup in res.scalars().all():
            taskgroup.resource_id = resource_id
            db.add(taskgroup)
        db.commit()
        logging.info(f"Set {resource_id=} foreign key for all task groups.")

        # Update projects
        res = db.execute(select(ProjectV2).order_by(ProjectV2.id))
        for project in res.scalars().all():
            project.resource_id = resource_id
            db.add(project)
        db.commit()
        logging.info(f"Set {resource_id=} foreign key for all projects.")
        print()

        db.expunge_all()

        for _, info in profile_and_user_updates.items():
            # Create profile
            profile_data = info.data
            profile_data["resource_id"] = resource_id
            profile = Profile(**profile_data)
            db.add(profile)
            db.commit()
            db.refresh(profile)
            db.expunge(profile)
            profile_id = profile.id
            logging.info(
                f"Created profile '{profile.name}', with {profile.id=}."
            )

            # Update users
            for user_update in info.user_updates:
                user = db.get(UserOAuth, user_update.user_id)
                user.profile_id = profile_id
                user.project_dir = user_update.project_dir
                user.slurm_accounts = user_update.slurm_accounts
                db.add(user)
                logging.info(f"Updated {user.email} with {user.project_dir=}.")
                logging.info(
                    f"Associated {user.email} to profile {profile.name}."
                )
            print()
            db.commit()

    logging.info("END - all ok.")
