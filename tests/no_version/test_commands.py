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


commands = [
    "fractalctl start --reload --host 0.0.0.0 --port 8000",
    (
        "gunicorn fractal_server.main:app "
        "--workers 1 "
        "--bind 0.0.0.0:8000 "
        "--worker-class fractal_server.gunicorn_fractal.FractalWorker "
        "--logger-class fractal_server.gunicorn_fractal.FractalGunicornLogger "
    ),
]


@pytest.mark.parametrize("cmd", commands)
def test_startup_commands(cmd, db_create_tables):
    debug(cmd)
    p = subprocess.Popen(
        shlex.split(f"poetry run {cmd}"),
        start_new_session=True,
        cwd=FRACTAL_SERVER_DIR,  # FIXME drop
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
        cwd=FRACTAL_SERVER_DIR,  # FIXME: Drop
    )
    assert not res.stdout
    assert "usage" in res.stderr

    cmd = (
        'printf "mypassword\n" | '
        "poetry run fractalctl encrypt-email-password"
    )
    res = subprocess.run(
        cmd,
        encoding="utf-8",
        capture_output=True,
        cwd=FRACTAL_SERVER_DIR,
        shell=True,
    )
    assert "FRACTAL_EMAIL_PASSWORD" in res.stdout
    assert "FRACTAL_EMAIL_PASSWORD_KEY" in res.stdout
    assert not res.stderr
