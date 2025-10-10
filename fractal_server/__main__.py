import argparse as ap
import asyncio
import json
import sys

import uvicorn


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
    "--resource-json-file",
    type=str,
    help="JSON file with a serialized first resource.",
    required=False,
)
init_db_data_parser.add_argument(
    "--profile-json-file",
    type=str,
    help="JSON file with a serialized first profile.",
    required=False,
)

# fractalctl update-db-data
update_db_data_parser = subparsers.add_parser(
    "update-db-data",
    description="Apply data-migration script to an existing database.",
)

# fractalctl encrypt-email-password
encrypt_email_password_parser = subparsers.add_parser(
    "encrypt-email-password",
    description=(
        "Generate valid values for environment variables "
        "FRACTAL_EMAIL_PASSWORD and FRACTAL_EMAIL_PASSWORD_KEY."
    ),
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
    resource_json_file: str | None,
    profile_json_file: str | None,
) -> None:
    from fractal_server.app.security import _create_first_user
    from fractal_server.app.security import _create_first_group
    from fractal_server.syringe import Inject
    from fractal_server.config import get_init_data_settings
    from fractal_server.app.db import get_sync_db
    from sqlalchemy import select
    from fractal_server.app.models.security import UserOAuth
    from fractal_server.app.models import Resource, Profile

    init_data_settings = Inject(get_init_data_settings)

    # Create default group and user
    print()
    _create_first_group()
    print()
    asyncio.run(
        _create_first_user(
            email=init_data_settings.FRACTAL_DEFAULT_ADMIN_EMAIL,
            password=(
                init_data_settings.FRACTAL_DEFAULT_ADMIN_PASSWORD.get_secret_value()  # noqa E501
            ),
            username=init_data_settings.FRACTAL_DEFAULT_ADMIN_USERNAME,
            is_superuser=True,
            is_verified=True,
        )
    )
    print()

    # Create first resource
    with next(get_sync_db()) as db:
        if resource_json_file is not None:
            with open(resource_json_file) as f:
                resource_data = json.load(f)
        else:
            resource_data = {
                "name": "Local resource",
                "type": "local",
                "job_local_folder": "data-jobs",
                "tasks_local_folder": "data-tasks",
            }
        resource = Resource(**resource_data)
        db.add(resource)
        db.commit()
        db.refresh(resource)
        if profile_json_file is not None:
            with open(profile_json_file) as f:
                profile_data = json.load(f)
        else:
            profile_data = {}
        profile_data["resource_id"] = resource.id
        profile = Profile(**profile_data)
        db.add(profile)
        db.commit()
        db.refresh(profile)

        res = db.execute(select(UserOAuth))
        users = res.unique().scalars().all()
        for user in users:
            print(f"Now setting profile_id={profile.id} for {user.email}.")
            user.profile_id = profile.id
            db.add(user)
        db.commit()
        db.expunge_all()

    # FIXME not tested yet


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


def print_encrypted_password():
    from cryptography.fernet import Fernet

    password = input("Insert email password: ").encode("utf-8")
    key = Fernet.generate_key().decode("utf-8")
    encrypted_password = Fernet(key).encrypt(password).decode("utf-8")

    print(f"\nFRACTAL_EMAIL_PASSWORD={encrypted_password}")
    print(f"FRACTAL_EMAIL_PASSWORD_KEY={key}")


def run():
    args = parser.parse_args(sys.argv[1:])

    if args.cmd == "openapi":
        save_openapi(dest=args.openapi_file)
    elif args.cmd == "set-db":
        set_db()
    elif args.cmd == "init-db-data":
        init_db_data(
            resource_json_file=args.resource_json_file,
            profile_json_file=args.profile_json_file,
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
    elif args.cmd == "encrypt-email-password":
        print_encrypted_password()
    else:
        sys.exit(f"Error: invalid command '{args.cmd}'.")


if __name__ == "__main__":
    run()
