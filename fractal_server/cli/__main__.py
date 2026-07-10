import sys
from argparse import Namespace

from ._init_db_data import init_db_data
from ._openapi import save_openapi
from ._parser import parse_args
from ._recent import recent
from ._set_db import set_db
from ._start import start
from ._sync_core_tasks import sync_core_tasks
from ._update_db_data import update_db_data


def run() -> None:
    args: Namespace = parse_args()
    match args.cmd:
        case "openapi":
            save_openapi(dest=args.openapi_file)
        case "set-db":
            set_db()
        case "init-db-data":
            init_db_data(
                resource=args.resource,
                profile=args.profile,
                admin_email=args.admin_email,
                admin_password=args.admin_pwd,
                admin_project_dir=args.admin_project_dir,
            )
        case "update-db-data":
            update_db_data()
        case "start":
            start(
                host=args.host,
                port=args.port,
                reload=args.reload,
            )
        case "recent":
            recent(minutes=args.minutes)
        case "sync-core-tasks":
            sync_core_tasks()
        case _:
            sys.exit(f"Error: invalid command '{args.cmd}'.")
