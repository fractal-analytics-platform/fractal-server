import asyncio
import json
import sys
from pathlib import Path

from pydantic import ValidationError
from sqlalchemy import func
from sqlalchemy import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import Profile
from fractal_server.app.models import Resource
from fractal_server.app.models.security import UserOAuth
from fractal_server.app.schemas.v2 import ResourceType
from fractal_server.app.schemas.v2.profile import cast_serialize_profile
from fractal_server.app.schemas.v2.resource import cast_serialize_resource
from fractal_server.app.security import _create_first_group
from fractal_server.app.security import _create_first_user


def init_db_data(
    *,
    resource: str | None = None,
    profile: str | None = None,
    admin_email: str | None = None,
    admin_password: str | None = None,
    admin_project_dir: str | None = None,
) -> None:
    # Create default group and user
    print()
    _create_first_group()
    print()

    # Create admin user if requested
    if not (
        (admin_email is None)
        == (admin_password is None)
        == (admin_project_dir is None)
    ):
        print(
            "You must provide either or or none of `--admin-email`, "
            "`--admin-pwd` and `--admin-project-dir`. Exit."
        )
        sys.exit(1)

    if (
        admin_email is not None
        and admin_password is not None
        and admin_project_dir is not None
    ):
        asyncio.run(
            _create_first_user(
                email=admin_email,
                password=admin_password,
                project_dir=admin_project_dir,
                is_superuser=True,
                is_verified=True,
            )
        )
        print()

    # Create resource and profile if requested
    if (resource is None) != (profile is None):
        print("You must provide both --resource and --profile. Exit.")
        sys.exit(1)
    if resource and profile:
        with next(get_sync_db()) as db:
            # Preliminary check
            num_resources = db.execute(select(func.count(Resource.id))).scalar()
            if num_resources != 0:
                print(f"There exist already {num_resources=} resources. Exit.")
                sys.exit(1)

            # Get resource/profile data
            if resource == "default":
                _python_version = (
                    f"{sys.version_info.major}.{sys.version_info.minor}"
                )
                resource_data = {
                    "name": "Local resource",
                    "type": ResourceType.LOCAL,
                    "jobs_local_dir": (Path.cwd() / "data-jobs").as_posix(),
                    "tasks_local_dir": (Path.cwd() / "data-tasks").as_posix(),
                    "tasks_python_config": {
                        "default_version": _python_version,
                        "versions": {
                            _python_version: sys.executable,
                        },
                    },
                    "jobs_poll_interval": 0,
                    "jobs_runner_config": {},
                    "tasks_pixi_config": {},
                }
                print("Prepared default resource data.")
            else:
                with open(resource) as f:
                    resource_data = json.load(f)
                print(f"Read resource data from {resource}.")
            if profile == "default":
                profile_data = {
                    "resource_type": "local",
                    "name": "Local profile",
                }
                print("Prepared default profile data.")
            else:
                with open(profile) as f:
                    profile_data = json.load(f)
                print(f"Read profile data from {profile}.")

            # Validate resource/profile data
            try:
                resource_data = cast_serialize_resource(resource_data)
            except ValidationError as e:
                sys.exit(
                    f"ERROR: Invalid resource data.\nOriginal error:\n{str(e)}"
                )
            try:
                profile_data = cast_serialize_profile(profile_data)
            except ValidationError as e:
                sys.exit(
                    f"ERROR: Invalid profile data.\nOriginal error:\n{str(e)}"
                )

            # Create resource/profile db objects
            resource_obj = Resource(**resource_data)
            db.add(resource_obj)
            db.commit()
            db.refresh(resource_obj)
            profile_data["resource_id"] = resource_obj.id
            profile_obj = Profile(**profile_data)
            db.add(profile_obj)
            db.commit()
            db.refresh(profile_obj)

            # Associate profile to users
            res = db.execute(select(UserOAuth))
            users = res.unique().scalars().all()
            for user in users:
                print(f"Now set profile_id={profile_obj.id} for {user.email}.")
                user.profile_id = profile_obj.id
                db.add(user)
            db.commit()
            db.expunge_all()
            print()
