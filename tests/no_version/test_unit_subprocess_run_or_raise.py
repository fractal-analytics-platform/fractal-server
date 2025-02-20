import pathlib

import pytest
from devtools import debug

from fractal_server.app.runner.exceptions import JobExecutionError
from fractal_server.app.runner.executors.slurm_sudo.executor import (
    _subprocess_run_or_raise,
)


def test_subprocess_run_or_raise(tmp_path: pathlib.Path):
    # Successfull call
    debug(tmp_path)
    with (tmp_path / "1.txt").open("w") as f:
        f.write("1\n")
    with (tmp_path / "2.txt").open("w") as f:
        f.write("2\n")

    cmd_ok = f"ls {str(tmp_path)}"
    debug(cmd_ok)
    output = _subprocess_run_or_raise(cmd_ok)
    debug(output)
    assert output.stderr == ""
    assert output.stdout.strip("\n").split("\n") == ["1.txt", "2.txt"]

    # Failed call
    cmd_fail = "ls --invalid-option"
    debug(cmd_fail)

    with pytest.raises(JobExecutionError) as e:
        _subprocess_run_or_raise(cmd_fail)
    debug(e)
    debug(e.value)
    debug(e.value.info)
    assert "ls: unrecognized option '--invalid-option'" in e.value.info
