import shlex
import subprocess
from pathlib import Path

import pytest
from devtools import debug


def _prepare_config_and_db(_tmp_path: Path):
    cwd = str(_tmp_path)

    config = "\n".join(
        [
            "DEPLOYMENT_TYPE=testing",
            "JWT_SECRET_KEY=secret",
            f"SQLITE_PATH={cwd}/test.db",
            "FRACTAL_LOGGING_LEVEL=10",
            "FRACTAL_RUNNER_BACKEND=local",
            f"FRACTAL_RUNNER_WORKING_BASE_DIR={cwd}/artifacts",
            f"FRACTAL_TASKS_DIR={cwd}/FRACTAL_TASKS_DIR",
            "FRACTAL_ADMIN_DEFAULT_EMAIL=admin@fractal.xy",
            "FRACTAL_ADMIN_DEFAULT_PASSWORD=1234",
            "JWT_EXPIRE_SECONDS=84600",
            "\n",
        ]
    )
    with (_tmp_path / ".fractal_server.env").open("w") as f:
        f.write(config)

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
    "gunicorn fractal_server.main:app --worker-class uvicorn.workers.UvicornWorker --bind localhost:8010",  # noqa
    "gunicorn fractal_server.main:app --worker-class uvicorn.workers.UvicornWorker --workers 2",  # noqa
]


@pytest.mark.parametrize("cmd", commands)
def test_startup_commands(cmd, tmp_path):

    _prepare_config_and_db(tmp_path)

    debug(cmd)
    with pytest.raises(subprocess.TimeoutExpired) as e:
        res = subprocess.run(
            shlex.split(cmd),
            encoding="utf-8",
            capture_output=True,
            cwd=str(tmp_path),
            timeout=2,
        )
        debug(res.stdout)
        debug(res.stderr)
        debug(res.returncode)
    debug(e.value)
