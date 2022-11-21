from concurrent.futures import Executor
from typing import Callable

import pytest
from devtools import debug

from fractal_server.app.runner._slurm.executor import FractalSlurmExecutor


def submit(executor: Executor, fun: Callable, *args, **kwargs):
    try:
        return executor.submit(fun, *args, **kwargs)
    except Exception as e:
        debug(e)


@pytest.mark.parametrize(("username"), [None, "my_user"])
def test_submit_pre_command(fake_process, username, tmp_path):
    """
    GIVEN a FractalSlurmExecutor
    WHEN it is initialised with / without a username
    THEN the sbatch call contains / does not contain the sudo pre-command
    """
    fake_process.register(["sbatch", fake_process.any()])
    fake_process.register(["sudo", fake_process.any()])

    with FractalSlurmExecutor(
        username=username, script_dir=tmp_path
    ) as executor:
        submit(executor, lambda: None)

    debug(fake_process.calls)
    call = fake_process.calls.pop()
    assert "sbatch" in call
    if username:
        assert f"sudo --non-interactive -u {username}" in call
