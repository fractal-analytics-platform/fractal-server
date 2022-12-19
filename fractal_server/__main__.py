import argparse as ap
from sys import argv

import uvicorn

parser = ap.ArgumentParser()

subparsers = parser.add_subparsers(title="Commands", dest="cmd", required=True)

# fractalctl start
startserver = subparsers.add_parser("start", help="Start the server")
startserver.add_argument("--host", default="127.0.0.1")
startserver.add_argument("-p", "--port", default=8000, type=int)
startserver.add_argument("--reload", default=False, action="store_true")

# fractalctl openapi
openapi_parser = subparsers.add_parser(
    "openapi", help="Save the `openapi.json` file"
)
openapi_parser.add_argument(
    "-f",
    "--openapi-file",
    help="Filename for OpenAPI schema dump",
    default="openapi.json",
)

# fractalctl set-db
subparsers.add_parser("set-db", help="Initialise the database")


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


def run():
    args = parser.parse_args(argv[1:])

    if args.cmd == "openapi":
        save_openapi(dest=args.openapi_file)
    elif args.cmd == "set-db":
        set_db()
    else:
        uvicorn.run(
            "fractal_server.main:app",
            host=args.host,
            port=args.port,
            reload=args.reload,
        )


if __name__ == "__main__":
    run()
