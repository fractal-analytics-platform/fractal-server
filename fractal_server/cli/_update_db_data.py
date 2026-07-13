import os
import sys
from importlib import import_module

from packaging.version import parse

import fractal_server


def update_db_data() -> None:
    """
    Apply data migrations.
    """

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
