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


def test_unit_sbatch_script_readable(monkey_slurm, tmp777_path):
    """
    GIVEN a batch script written to file by the slurm executor
    WHEN a differnt user tries to read it
    THEN it has all the permissions needed
    """
    from fractal_server.app.runner._slurm.executor import write_batch_script
    import subprocess
    import shlex

    SBATCH_SCRIPT = "test"
    f = write_batch_script(SBATCH_SCRIPT, script_dir=tmp777_path)

    out = subprocess.run(
        # f"sudo --non-interactive -u test01 cat {f}",
        shlex.split(f"sudo --non-interactive -u test01 cat {f}"),
        capture_output=True,
        text=True,
    )
    debug(out.stderr)
    assert out.returncode == 0
    debug(out)
    assert out.stdout == SBATCH_SCRIPT


@pytest.mark.parametrize("username", [None, "test01"])
def test_slurm_executor(username, monkey_slurm, tmp777_path):
    """
    GIVEN a slurm cluster in a docker container
    WHEN a function is submitted to the cluster executor, as a given user
    THEN the result is correctly computed
    """

    with FractalSlurmExecutor(
        script_dir=tmp777_path, username=username
    ) as executor:
        res = executor.submit(lambda: 42)
    assert res.result() == 42
