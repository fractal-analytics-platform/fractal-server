import pathlib

from devtools import debug

from fractal_server.app.runner._slurm.executor import _subprocess_run_or_raise
from fractal_server.app.runner.common import JobExecutionError


def test_subprocess_run_or_raise(tmp_path: pathlib.Path):
    # Successfull call
    debug(tmp_path)
    with (tmp_path / "1.txt").open("w") as f:
        f.write("1\n")
    with (tmp_path / "2.txt").open("w") as f:
        f.write("2\n")

    cmd_ok = f"ls {str(tmp_path)}"
    debug(cmd_ok)
    cmd_fail = "ls --invalid-option"
    output = _subprocess_run_or_raise(cmd_ok)
    debug(output)
    assert output.stderr == ""
    assert output.stdout.strip("\n").split("\n") == ["1.txt", "2.txt"]

    # Failed call
    try:
        _subprocess_run_or_raise(cmd_fail)
        raise RuntimeError("This branch should have never been reached.")
    except JobExecutionError as e:
        debug(e.info)
        assert "ls: unrecognized option '--invalid-option'" in e.info
