import os
import shlex
import signal
import subprocess
import time

import pytest
from devtools import debug

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
        shlex.split(f"uv run --frozen {cmd}"),
        start_new_session=True,
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
    cmd = "uv run --frozen fractalctl email-settings"
    res = subprocess.run(
        shlex.split(cmd),
        encoding="utf-8",
        capture_output=True,
    )
    assert not res.stdout
    assert "usage" in res.stderr
