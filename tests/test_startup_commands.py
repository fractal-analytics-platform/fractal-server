import os
import shlex
import signal
import subprocess
import time
from pathlib import Path

import pytest
from devtools import debug

from .fixtures_server import DB_ENGINE


def _prepare_config_and_db(_tmp_path: Path):
    cwd = str(_tmp_path)

    # General config
    config_lines = [
        "DEPLOYMENT_TYPE=testing",
        "JWT_SECRET_KEY=secret",
        "FRACTAL_LOGGING_LEVEL=10",
        "FRACTAL_RUNNER_BACKEND=local",
        f"FRACTAL_RUNNER_WORKING_BASE_DIR={cwd}/artifacts",
        f"FRACTAL_TASKS_DIR={cwd}/FRACTAL_TASKS_DIR",
        "FRACTAL_ADMIN_DEFAULT_EMAIL=admin@fractal.xy",
        "FRACTAL_ADMIN_DEFAULT_PASSWORD=1234",
        "JWT_EXPIRE_SECONDS=84600",
        f"DB_ENGINE={DB_ENGINE}",
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
        config_lines.append(f"SQLITE_PATH={cwd}/test.db")
    else:
        raise ValueError(f"Invalid {DB_ENGINE=}")

    # Write config to file
    config = "\n".join(config_lines + ["\n"])
    with (_tmp_path / ".fractal_server.env").open("w") as f:
        f.write(config)

    # Initialize db
    cmd = "fractalctl set-db"
    debug(cmd)
    res = subprocess.run(
        shlex.split(cmd),
        encoding="utf-8",
        capture_output=True,
        cwd=cwd,
    )
    debug(res.stdout)
    debug(res.stderr)
    debug(res.returncode)
    assert res.returncode == 0


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
        shlex.split(cmd),
        start_new_session=True,
        cwd=str(tmp_path),
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
