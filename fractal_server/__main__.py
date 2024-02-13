import argparse as ap
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
subparsers.add_parser(
    "set-db",
    description="Initialise the database and apply schema migrations",
)

# fractalctl update-db-data
subparsers.add_parser(
    "update-db-data",
    description="Apply data-migration script to an existing database.",
)


def save_openapi(dest="openapi.json"):
    from fractal_server.main import start_application
    import json

    app = start_application()
    openapi_schema = app.openapi()

    with open(dest, "w") as f:
        json.dump(openapi_schema, f)


def set_db():
    """
    Set-up / Upgrade database schema

    Call alembic to upgrade to the latest migration.

    Ref: https://stackoverflow.com/a/56683030/283972
    """
    import alembic.config
    from pathlib import Path
    import fractal_server

    alembic_ini = Path(fractal_server.__file__).parent / "alembic.ini"
    alembic_args = ["-c", alembic_ini.as_posix(), "upgrade", "head"]

    print(f"Run alembic.config, with argv={alembic_args}")
    alembic.config.main(argv=alembic_args)


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
