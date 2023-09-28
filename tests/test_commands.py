import os
import shlex
import signal
import subprocess
import time
from pathlib import Path

import pytest
from devtools import debug

import fractal_server
from .fixtures_server import DB_ENGINE


FRACTAL_SERVER_DIR = Path(fractal_server.__file__).parent


def _prepare_config_and_db(_tmp_path: Path):
    if not _tmp_path.exists():
        _tmp_path.mkdir(parents=True)

    cwd = str(_tmp_path)

    # General config
    config_lines = [
        f"DB_ENGINE={DB_ENGINE}",
        f"FRACTAL_TASKS_DIR={cwd}/FRACTAL_TASKS_DIR",
        f"FRACTAL_RUNNER_WORKING_BASE_DIR={cwd}/artifacts",
        "JWT_SECRET_KEY=secret",
        "FRACTAL_LOGGING_LEVEL=10",
    ]

    # DB_ENGINE-specific config
    if DB_ENGINE == "postgres":
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


def test_set_db(tmp_path: Path):
    """
    Run `poetry run fractalctl set-db`
    """
    _prepare_config_and_db(tmp_path)
    if DB_ENGINE == "sqlite":
        db_file = str(tmp_path / "test.db")
        debug(db_file)
        assert os.path.exists(db_file)


def test_alembic_check(tmp_path):
    """
    Run `poetry run alembic check` to see whether new migrations are needed
    """
    # Set db
    db_folder = tmp_path / "test_alembic_check"
    _prepare_config_and_db(db_folder)

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
def test_startup_commands(cmd, tmp_path):

    _prepare_config_and_db(tmp_path)

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
