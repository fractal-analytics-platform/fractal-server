"""
Preliminary checks:
1. Env variables (how should they be set?)
2. All non-relevant users must be marked as non-active in advance.

NEEDED
New .env
Old .env
Old slurm config
Old pixi config


POST UPDATE:
* Rename resource
* Rename profile
"""
import json
import logging
from typing import Any

from devtools import debug
from dotenv.main import DotEnv
from sqlalchemy.sql.operators import is_
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
from fractal_server.runner.config import JobRunnerConfigSLURM
from fractal_server.tasks.config import TasksPixiSettings
from fractal_server.tasks.config import TasksPythonSettings


logger = logging.getLogger("fix_db")
logger.setLevel(logging.INFO)


def update_profiles_slurm_ssh(
    *,
    resource_id: int,
    users: list[UserOAuth],
):
    with next(get_sync_db()) as db:
        for user in users:
            debug("OLD", user)
            if user.user_settings_id is None:
                logger.warning(f"{user.email=} has {user.user_settings_id=}")
                user.project_dir = "/PLACEHOLDER"
            else:
                user_settings = db.get(UserSettings, user.user_settings_id)
                debug(user_settings)
                if user_settings.project_dir is None:
                    logger.warning(
                        f"User {user.email} has {user_settings.project_dir=}."
                    )
                    user.project_dir = "/PLACEHOLDER"
                else:
                    user.project_dir = user_settings.project_dir
                user.slurm_accounts = user_settings.slurm_accounts or []
            debug("NEW", user)
            db.merge(user)
        db.commit()

    # PROFILES = [dict(data=..., user_ids=[1, 2, 3])]
    #     print(f"START updating user {user.email}")

    #     # profile_id
    #     if user_settings.ssh_username is None:
    #         sys.exit(f"User {user.email} has {user_settings.ssh_username=}. Exit.")
    #     if user_settings.ssh_private_key_path is None:
    #         sys.exit(
    #             f"User {user.email} has "
    #             f"{user_settings.ssh_private_key_path=}. "
    #             "Exit."
    #         )
    #     if user_settings.ssh_host is None:
    #         sys.exit(f"User {user.email} has {user_settings.ssh_host=}. Exit.")

    #     profile_id = PROFILE_IDS.get(user_settings.ssh_username, None)
    #     if profile_id is None:
    #         profile = Profile(
    #             resource_id=resource_id,
    #             resource_type=settings.FRACTAL_RUNNER_BACKEND,
    #             username=user_settings.ssh_username,
    #             host=user_settings.ssh_host,
    #             ssh_key_path=user_settings.ssh_private_key_path,
    #             name=f"{user_settings.ssh_username} profile",
    #             jobs_remote_dir="/fake",  # FIXME
    #             tasks_remote_dir="/fake",  # FIXME
    #         )
    #         db.add(profile)
    #         db.commit()
    #         db.refresh(profile)
    #         db.expunge(profile)
    #         profile_id = profile.id
    #         PROFILE_IDS[user_settings.ssh_username] = profile_id
    #     user.profile_id = profile_id
    #     db.add(user)
    #     print(f"END   updating user {user.email}")


def get_old_dotenv_variables() -> dict[str, str | None]:
    """
    See
    https://github.com/fractal-analytics-platform/fractal-server/blob/2.16.x/fractal_server/config.py
    """
    OLD_DOTENV_FILE = ".fractal_server.env.old"
    return dict(**DotEnv(dotenv_path=OLD_DOTENV_FILE).dict())


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
    obj = TasksPixiSettings(**old_pixi_config)
    return obj.model_dump()


def get_JobRunnerConfigSLURM(
    old_config: dict[str, str | None]
) -> dict[str, Any]:
    slurm_file = old_config["FRACTAL_SLURM_CONFIG_FILE"]
    with open(slurm_file) as f:
        old_slurm_config = json.load(f)
    obj = JobRunnerConfigSLURM(**old_slurm_config)
    return obj.model_dump()


def get_ssh_host() -> str:
    with next(get_sync_db()) as db:
        res = db.execute(select(UserSettings.ssh_host))
        hosts = res.scalars().all()
    if len(set(hosts)) > 1:
        host = max(set(hosts), key=hosts.count)
        print(f"MOST FREQUENT HOST: {host}")
    else:
        host = hosts[0]
    return host


def get_Resource(old_config: dict[str, str | None]) -> dict[str, Any]:
    settings = get_settings()

    resource_data = dict(
        type=settings.FRACTAL_RUNNER_BACKEND,
        name="Resource Name",
        tasks_python_config=get_TasksPythonSettings(old_config),
        tasks_pixi_config=get_TasksPixiSettings(old_config),
        tasks_local_dir=old_config["FRACTAL_TASKS_DIR"],
        jobs_local_dir=old_config["FRACTAL_RUNNER_WORKING_BASE_DIR"],
        jobs_runner_config=get_JobRunnerConfigSLURM(old_config),
        jobs_poll_interval=int(old_config.get("FRACTAL_SLURM_POLL_INTERVAL")),
        jobs_slurm_python_worker=old_config["FRACTAL_SLURM_WORKER_PYTHON"],
    )
    if settings.FRACTAL_RUNNER_BACKEND == "slurm_ssh":
        resource_data["host"] = get_ssh_host()

    resource_data = cast_serialize_resource(resource_data)

    return resource_data


def fix_db():
    settings = get_settings()
    if settings.FRACTAL_RUNNER_BACKEND == "local":
        raise NotImplementedError()
    old_config = get_old_dotenv_variables()
    debug(old_config)
    resource_data = get_Resource(old_config)

    with next(get_sync_db()) as db:
        # Create new resource
        resource = Resource(**resource_data)
        db.add(resource)
        db.commit()
        db.refresh(resource)
        db.expunge(resource)
        resource_id = resource.id
        debug(f"CREATED RESOURCE with {resource_id=}")

        # Update task groups
        res = db.execute(select(TaskGroupV2).order_by(TaskGroupV2.id))
        for taskgroup in res.scalars().all():
            taskgroup.resource_id = resource_id
            db.add(taskgroup)
        db.commit()

        # Update projects
        res = db.execute(select(ProjectV2).order_by(ProjectV2.id))
        for project in res.scalars().all():
            project.resource_id = resource_id
            db.add(project)
        db.commit()

        # Get active users
        res = db.execute(
            select(UserOAuth)
            .where(is_(UserOAuth.is_active, True))
            .order_by(UserOAuth.id)
        )
        users = list(res.unique().scalars().all())

        update_profiles_slurm_ssh(
            users=users,
            resource_id=resource_id,
        )
