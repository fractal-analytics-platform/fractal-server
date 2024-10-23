import os
import shlex
import subprocess
from pathlib import Path

import pytest
from devtools import debug

import fractal_server
from tests.fixtures_server import DB_ENGINE

FRACTAL_SERVER_DIR = Path(fractal_server.__file__).parent


@pytest.fixture(scope="function")
async def set_test_db(tmp_path):

    if not tmp_path.exists():
        tmp_path.mkdir(parents=True)

    cwd = str(tmp_path)

    # General config
    config_lines = [
        f"DB_ENGINE={DB_ENGINE}",
        f"FRACTAL_TASKS_DIR={cwd}/FRACTAL_TASKS_DIR",
        f"FRACTAL_RUNNER_WORKING_BASE_DIR={cwd}/artifacts",
        "JWT_SECRET_KEY=secret",
        "FRACTAL_LOGGING_LEVEL=10",
    ]

    # DB_ENGINE-specific config
    if DB_ENGINE == "postgres-psycopg":
        config_lines.extend(
            [
                "POSTGRES_USER=postgres",
                "POSTGRES_PASSWORD=postgres",
                "POSTGRES_DB=fractal_test",
            ]
        )
    elif DB_ENGINE == "sqlite":
        if "SQLITE_PATH" in os.environ:
            SQLITE_PATH = os.environ.pop("SQLITE_PATH")
            debug(f"Dropped {SQLITE_PATH=} from `os.environ`.")
        config_lines.append(f"SQLITE_PATH={cwd}/test.db")
        debug(f"SQLITE_PATH={cwd}/test.db")
    else:
        raise ValueError(f"Invalid {DB_ENGINE=}")

    # Write config to file
    config = "\n".join(config_lines + ["\n"])
    with (FRACTAL_SERVER_DIR / ".fractal_server.env").open("w") as f:
        f.write(config)

    # Initialize db
    cmd = "poetry run fractalctl set-db"
    debug(cmd)
    res = subprocess.run(
        shlex.split(cmd),
        encoding="utf-8",
        capture_output=True,
        cwd=FRACTAL_SERVER_DIR,
    )
    debug(res.stdout)
    debug(res.stderr)
    debug(res.returncode)
    assert res.returncode == 0

    yield

    if DB_ENGINE == "postgres-psycopg":
        # Apply migrations on reverse until database is dropped, in order to
        # keep tests stateless:
        # https://alembic.sqlalchemy.org/en/latest/tutorial.html#downgrading
        # Not required by SQLite. In that case we do not remove the `test.db`
        # file, which can be read after the test.
        cmd = "poetry run alembic downgrade base"
        res = subprocess.run(
            shlex.split(cmd),
            encoding="utf-8",
            capture_output=True,
            cwd=FRACTAL_SERVER_DIR,
        )
        debug(res.stdout)
        debug(res.stderr)
        debug(res.returncode)
        assert res.returncode == 0
    # Removing env file (to keep tests stateless)
    Path.unlink(FRACTAL_SERVER_DIR / ".fractal_server.env")
