from pathlib import Path

import pytest

from fractal_server.app.runner.run_subprocess import run_subprocess


def test_run_subprocess(tmp_path: Path):
    # Success
    res = run_subprocess(f"ls {tmp_path.as_posix()}")
    assert res.returncode == 0
    # Failure
    with pytest.raises(Exception, match="No such file or directory"):
        run_subprocess("/invalid_cmd")
