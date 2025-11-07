import argparse as ap
import asyncio
import json
import sys
from pathlib import Path

import uvicorn
from pydantic import ValidationError


parser = ap.ArgumentParser(description="fractal-server commands")

subparsers = parser.add_subparsers(title="Commands", dest="cmd", required=True)

# fractalctl start
startserver = subparsers.add_parser(
    "start", description="Start the server (with uvicorn)"
)
startserver.add_argument(
    "--host",
    default="127.0.0.1",
    type=str,
    help="bind socket to this host (default: 127.0.0.1)",
)
startserver.add_argument(
    "-p",
    "--port",
    default=8000,
    type=int,
    help="bind socket to this port (default: 8000)",
)
startserver.add_argument(
    "--reload", default=False, action="store_true", help="enable auto-reload"
)

# fractalctl openapi
openapi_parser = subparsers.add_parser(
    "openapi", description="Save the `openapi.json` file"
)
openapi_parser.add_argument(
    "-f",
    "--openapi-file",
    type=str,
    help="Filename for OpenAPI schema dump",
    default="openapi.json",
)

# fractalctl set-db
set_db_parser = subparsers.add_parser(
    "set-db",
    description=(
        "Initialise/upgrade database schemas and create first group&user."
    ),
)

# fractalctl init-db-data
init_db_data_parser = subparsers.add_parser(
    "init-db-data",
    description="Populate database with initial data.",
)
init_db_data_parser.add_argument(
    "--resource",
    type=str,
    help="Either `default` or path to the JSON file of the first resource.",
    required=False,
)
init_db_data_parser.add_argument(
    "--profile",
    type=str,
    help="Either `default` or path to the JSON file of the first profile.",
    required=False,
)
init_db_data_parser.add_argument(
    "--admin-email",
    type=str,
    help="Email of the first admin user.",
    required=False,
)
init_db_data_parser.add_argument(
    "--admin-pwd",
    type=str,
    help="Password for the first admin user.",
    required=False,
)

init_db_data_parser.add_argument(
    "--admin-project-dir",
    type=str,
    help="Project_dir for the first admin user.",
    required=False,
)

# fractalctl update-db-data
update_db_data_parser = subparsers.add_parser(
    "update-db-data",
    description="Apply data-migration script to an existing database.",
)


def save_openapi(dest="openapi.json"):
    from fractal_server.main import start_application

    app = start_application()
    openapi_schema = app.openapi()

    with open(dest, "w") as f:
        json.dump(openapi_schema, f)


def set_db():
    """
    Upgrade database schemas.

    Call alembic to upgrade to the latest migration.
    Ref: https://stackoverflow.com/a/56683030/283972
    """
    from fractal_server.syringe import Inject
    from fractal_server.config import get_db_settings

    import alembic.config
    from pathlib import Path
    import fractal_server

    # Validate DB settings
    Inject(get_db_settings)

    # Perform migrations
    alembic_ini = Path(fractal_server.__file__).parent / "alembic.ini"
    alembic_args = ["-c", alembic_ini.as_posix(), "upgrade", "head"]
    print(f"START: Run alembic.config, with argv={alembic_args}")
    alembic.config.main(argv=alembic_args)
    print("END: alembic.config")


def init_db_data(
    *,
    resource: str | None = None,
    profile: str | None = None,
    admin_email: str | None = None,
    admin_password: str | None = None,
    admin_project_dir: str | None = None,
) -> None:
    from fractal_server.app.security import _create_first_user
    from fractal_server.app.security import _create_first_group
    from fractal_server.app.db import get_sync_db
    from sqlalchemy import select, func
    from fractal_server.app.models.security import UserOAuth
    from fractal_server.app.models import Resource, Profile
    from fractal_server.app.schemas.v2.resource import cast_serialize_resource
    from fractal_server.app.schemas.v2.profile import cast_serialize_profile
    from fractal_server.app.schemas.v2 import ResourceType

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
    if admin_password and admin_email:
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
            num_resources = db.execute(
                select(func.count(Resource.id))
            ).scalar()
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


def update_db_data():
    """
    Apply data migrations.
    """

    import fractal_server
    from importlib import import_module
    from packaging.version import parse
    import os

    def _slugify_version(raw_version: str) -> str:
        v = parse(raw_version)
        return f"{v.major}_{v.minor}_{v.micro}"

    current_version = fractal_server.__VERSION__
    current_version_slug = _slugify_version(current_version)

    print(
        "**WARNING**\nThis command acts directly on database data. "
        "If you have any doubt about this, do not run it!\n"
    )

    print(
        "Expected use case:\n"
        "You have updated fractal-server from some old version to a "
        "target version, and now need to fix some database data.\n"
        f"The detected target version is '{current_version}' (corresponding "
        f"to the update-db-data module '{current_version_slug}.py').\n"
        "Note that the old and target versions MUST be consecutive, "
        "that is, you cannot skip an intermediate version.\nThe list of "
        "released versions is available at https://github.com/"
        "fractal-analytics-platform/fractal-server/blob/main/CHANGELOG.md."
    )
    print()

    if not os.path.isfile(".fractal_server.env"):
        sys.exit(
            "This command will look for a '.fractal_server.env' file, but "
            "there isn't one in the current directory."
        )

    yes_no_questions = (
        "Do you confirm that the old and target versions are consecutive?",
        f"Do you confirm that the target version is {current_version}?",
        "Have you run 'fractalctl set-db' with the target version?",
        "Have you made a backup of your database (e.g. via `pg_dump`)?",
        "Are you sure you want to proceed?",
    )
    for question in yes_no_questions:
        user_input = input(f"{question} (yes/no)\n")
        if user_input != "yes":
            sys.exit(f"Answer was '{user_input}'; exit.")

    try:
        current_update_db_data_module = import_module(
            f"fractal_server.data_migrations.{current_version_slug}"
        )
    except ModuleNotFoundError as e:
        print(
            f"Update-db module for version {current_version} not found; "
            f"exit.\nOriginal error message: {str(e)}"
        )
        sys.exit()

    print("OK, now starting data-migration script\n")
    current_update_db_data_module.fix_db()


def run():
    args = parser.parse_args(sys.argv[1:])

    if args.cmd == "openapi":
        save_openapi(dest=args.openapi_file)
    elif args.cmd == "set-db":
        set_db()
    elif args.cmd == "init-db-data":
        init_db_data(
            resource=args.resource,
            profile=args.profile,
            admin_email=args.admin_email,
            admin_password=args.admin_pwd,
            admin_project_dir=args.admin_project_dir,
        )
    elif args.cmd == "update-db-data":
        update_db_data()
    elif args.cmd == "start":
        uvicorn.run(
            "fractal_server.main:app",
            host=args.host,
            port=args.port,
            reload=args.reload,
        )
    else:
        sys.exit(f"Error: invalid command '{args.cmd}'.")


if __name__ == "__main__":
    run()
