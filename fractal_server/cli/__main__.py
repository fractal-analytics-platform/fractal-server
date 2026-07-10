import sys

from ._init_db_data import init_db_data
from ._openapi import save_openapi
from ._parser import parse_args
from ._recent import recent
from ._set_db import set_db
from ._start import start
from ._update_db_data import update_db_data


def run(args) -> None:
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
        start(
            host=args.host,
            port=args.port,
            reload=args.reload,
        )
    elif args.cmd == "recent":
        recent(minutes=args.minutes)
    else:
        sys.exit(f"Error: invalid command '{args.cmd}'.")


if __name__ == "__main__":
    args = parse_args()
    run(args)
