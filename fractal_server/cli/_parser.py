import argparse as ap
import sys
from pathlib import Path


def get_parser() -> ap.ArgumentParser:
    parser = ap.ArgumentParser(description="fractal-server commands")

    subparsers = parser.add_subparsers(
        title="Commands", dest="cmd", required=True
    )

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
        "--reload",
        default=False,
        action="store_true",
        help="enable auto-reload",
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
    set_db_parser = subparsers.add_parser(  # noqa: F841
        "set-db",
        description="Initialise/upgrade database schemas.",
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
    update_db_data_parser = subparsers.add_parser(  # noqa: F841
        "update-db-data",
        description="Apply data-migration script to an existing database.",
    )

    # fractalctl recent
    recent_parser = subparsers.add_parser(
        "recent",
        description="Review recent jobs and task-group activities.",
    )
    recent_parser.add_argument(
        "--minutes",
        type=int,
        help="Look-back period in minutes (default: 20).",
        default=20,
    )

    # fractalctl sync-core-tasks
    sync_core_tasks_parser = subparsers.add_parser(  # noqa: F841
        "sync-core-tasks",
        description="Synchronize core tasks.",
    )
    sync_core_tasks_parser.add_argument(
        "--resources-and-groups",
        type=Path,
        required=True,
        help=(
            "Path to a JSON file with an array of "
            '`{"resource_id": .., "user_group_id": ...}` objects. '
            "For each resource, only tasks associated to the corresponding user"
            " group can be marked as core."
        ),
    )
    sync_core_tasks_parser.add_argument(
        "--base",
        type=Path,
        required=False,
        help=(
            "Path to a JSON file with an array of items like "
            '`["my-package-name", "1.2.3", "My task name"]` which should be'
            " made core. This file is typically pre-fetched from a centralized"
            " source."
        ),
    )
    sync_core_tasks_parser.add_argument(
        "--additions",
        type=Path,
        required=False,
        help=(
            "Path to a JSON file with an array of items like "
            '`["my-package-name", "1.2.3", "My task name"]` which should be'
            " made core. This file is typically an instance-specific set of "
            " additions on top of the `--base` one."
        ),
    )
    sync_core_tasks_parser.add_argument(
        "--removals",
        type=Path,
        required=False,
        help=(
            "Path to a JSON file with an array of items like "
            '`["my-package-name", "1.2.3", "My task name"]` which should be'
            " made **not** core. This file is typically an instance-specific "
            "set of removals from the `--base` one."
        ),
    )

    return parser


def parse_args() -> ap.Namespace:
    parser = get_parser()
    args: ap.Namespace = parser.parse_args(sys.argv[1:])
    return args
