import shlex
import subprocess
from pathlib import Path

import pytest
from devtools import debug

import fractal_server

FRACTAL_SERVER_DIR = Path(fractal_server.__file__).parent


@pytest.fixture(scope="function")
async def set_test_db(tmp_path):
    if not tmp_path.exists():
        tmp_path.mkdir(parents=True)

    cwd = str(tmp_path)

    # General config
    config_lines = [
        f"FRACTAL_TASKS_DIR={cwd}/FRACTAL_TASKS_DIR",
        f"FRACTAL_RUNNER_WORKING_BASE_DIR={cwd}/artifacts",
        "JWT_SECRET_KEY=secret",
        "FRACTAL_LOGGING_LEVEL=10",
        "POSTGRES_USER=postgres",
        "POSTGRES_PASSWORD=postgres",
        "POSTGRES_DB=fractal_test",
    ]

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

    # Apply migrations on reverse until database is dropped, in order to
    # keep tests stateless:
    # https://alembic.sqlalchemy.org/en/latest/tutorial.html#downgrading
    # NOTE: We only run `alembic downgrade` until the specific revision that
    # drops V1 tables (rather than all the way to `base`), because that
    # revision is breakingly non-reversible
    cmd = "poetry run alembic downgrade 1eac13a26c83"
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
