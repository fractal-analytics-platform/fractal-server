import os
import shlex
import signal
import subprocess
import time
from pathlib import Path

import pytest
from devtools import debug

import fractal_server


FRACTAL_SERVER_DIR = Path(fractal_server.__file__).parent


def test_alembic_check(set_test_db):
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
    "fractalctl start --reload --host 0.0.0.0 --port 8000",
    (
        "gunicorn fractal_server.main:app "
        "--bind 0.0.0.0:8000 "
        "--workers 2 "
        "--worker-class uvicorn.workers.UvicornWorker "
    ),
    (
        "gunicorn fractal_server.main:app "
        "--workers 2 "
        "--bind 0.0.0.0:8000 "
        "--worker-class fractal_server.gunicorn_fractal.FractalWorker "
        "--logger-class "
        "fractal_server.gunicorn_fractal.FractalGunicornLogger "
    ),
]


@pytest.mark.parametrize("cmd", commands)
def test_startup_commands(cmd, set_test_db):

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


def test_email_settings():

    cmd = "poetry run fractalctl email-settings"
    res = subprocess.run(
        shlex.split(cmd),
        encoding="utf-8",
        capture_output=True,
        cwd=FRACTAL_SERVER_DIR,
    )
    assert not res.stdout
    assert "usage" in res.stderr

    cmd = (
        'printf "mypassword\n" | '
        "poetry run fractalctl email-settings "
        "sender@test.org localhost 1234 exact --skip-starttls"
    )
    res = subprocess.run(
        cmd,
        encoding="utf-8",
        capture_output=True,
        cwd=FRACTAL_SERVER_DIR,
        shell=True,
    )
    assert "FRACTAL_EMAIL_SETTINGS" in res.stdout
    assert "FRACTAL_EMAIL_SETTINGS_KEY" in res.stdout
    assert not res.stderr
