import os
import shlex
import shutil
import signal
import sqlite3
import subprocess
import time
from pathlib import Path

import pytest
from devtools import debug

import fractal_server
from .fixtures_server import DB_ENGINE


FRACTAL_SERVER_DIR = Path(fractal_server.__file__).parent


def test_set_db(tmp_path: Path, prepare_config_and_db):
    """
    Run `poetry run fractalctl set-db`
    """
    debug(DB_ENGINE)
    if DB_ENGINE == "sqlite":
        db_file = str(tmp_path / "test.db")
        debug(db_file)
        assert os.path.exists(db_file)


def test_alembic_check(prepare_config_and_db):
    """
    Run `poetry run alembic check` to see whether new migrations are needed
    """

    # Run check
    cmd = "poetry run alembic check"

    debug(cmd)
    res = subprocess.run(
        shlex.split(cmd),
        encoding="utf-8",
        capture_output=True,
        cwd=FRACTAL_SERVER_DIR,
    )
    debug(res.stdout)
    debug(res.stderr)
    if not res.returncode == 0:
        raise ValueError(
            f"Command `{cmd}` failed with exit code {res.returncode}.\n"
            f"Original stdout: {res.stdout}\n"
            f"Original stderr: {res.stderr}\n"
        )
    assert "No new upgrade operations detected" in res.stdout


commands = [
    "fractalctl start",
    "fractalctl start --reload",
    "fractalctl start --port 8000",
    "fractalctl start --host 0.0.0.0 --port 8000",
    "gunicorn fractal_server.main:app --worker-class uvicorn.workers.UvicornWorker --bind localhost:8000",  # noqa
    "gunicorn fractal_server.main:app --worker-class uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000",  # noqa
    "gunicorn fractal_server.main:app --worker-class uvicorn.workers.UvicornWorker --workers 8",  # noqa
]


@pytest.mark.parametrize("cmd", commands)
def test_startup_commands(cmd, prepare_config_and_db):

    debug(cmd)
    p = subprocess.Popen(
        shlex.split(f"poetry run {cmd}"),
        start_new_session=True,
        cwd=FRACTAL_SERVER_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
    )
    debug(p)
    with pytest.raises(subprocess.TimeoutExpired) as e:
        p.wait(timeout=1.5)
        out, err = p.communicate()
        debug(p.returncode)
        debug(out)
        debug(err)
        assert p.returncode == 0

    debug("Now call killpg")
    debug(p)
    os.killpg(os.getpgid(p.pid), signal.SIGTERM)
    # Wait a bit, so that the killpg ends before pytest ends
    time.sleep(0.3)
    debug(e.value)


def test_migrations_on_old_data(tmp_path: Path, testdata_path: Path):
    """
    1. Retrieve a database created with fractal-server 1.3.11
    2. Apply set-db, to update it to the current version
    3. Check that migrations go through with no error
    4. Check something about data

    Notes:
    * Step 4 is not general, but it specifically addresses issue #900.
    * For step 4 we are currently using direct sqlite3 access, to avoid relying
      on fractal-server for this simple check.
    """

    # 1. Retrieve a database created with fractal-server 1.3.11
    DBFILE = "test.db"
    shutil.copy(testdata_path / "test-1.3.11.db", tmp_path / DBFILE)
    cwd = str(tmp_path)

    # 2. Apply set-db, to update it to the current version
    config_lines = [
        "DB_ENGINE=sqlite",
        f"SQLITE_PATH={cwd}/{DBFILE}",
        f"FRACTAL_TASKS_DIR={cwd}/FRACTAL_TASKS_DIR",
        f"FRACTAL_RUNNER_WORKING_BASE_DIR={cwd}/artifacts",
        "JWT_SECRET_KEY=secret",
        "FRACTAL_LOGGING_LEVEL=10",
    ]
    debug(f"SQLITE_PATH={cwd}/test.db")
    config = "\n".join(config_lines + ["\n"])
    with (FRACTAL_SERVER_DIR / ".fractal_server.env").open("w") as f:
        f.write(config)
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

    # 3. Check that migrations go through with no error
    assert res.returncode == 0

    # 4. Check something about data
    con = sqlite3.connect(tmp_path / DBFILE)
    cur = con.cursor()
    out = cur.execute("SELECT history FROM dataset")
    history_column = out.fetchall()
    history_column_flat = [item[0] for item in history_column]
    assert None not in history_column_flat
