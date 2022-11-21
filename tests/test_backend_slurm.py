import shlex
from concurrent.futures import Executor
from typing import Callable
from typing import List

import pytest
from devtools import debug

from .fixtures_tasks import execute_command
from fractal_server.app.runner._slurm.executor import FractalSlurmExecutor


@pytest.fixture
async def slurm_container() -> str:
    try:
        output = await execute_command("docker ps")
        slurm_master = next(
            ln for ln in output.splitlines() if "slurm-docker-master" in ln
        )
        container_name = slurm_master.split()[-1]
        debug(container_name)
        return container_name
    except RuntimeError:
        pytest.xfail(reason="No Slurm master container found")


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


@pytest.fixture
def monkey_popen(slurm_container, monkeypatch):
    import subprocess

    OrigPopen = subprocess.Popen

    OVERRIDE_CMD = ["sbatch", "env"]

    class PopenLog:
        calls: List[OrigPopen] = []

        @classmethod
        def add_call(cls, call):
            cls.calls.append(call)

        @classmethod
        def last_call(cls):
            return cls.calls[-1].args

    class _MockPopen(OrigPopen):
        def __init__(self, *args, **kwargs):
            cmd = args[0]
            if not isinstance(cmd, list):
                cmd = shlex.split(cmd)

            if cmd[0] in OVERRIDE_CMD:
                cmd = ["docker", "exec", slurm_container] + cmd
            super().__init__(cmd, *args[1:], **kwargs)
            debug(shlex.join(self.args))
            PopenLog.add_call(self)

    monkeypatch.setattr(subprocess, "Popen", _MockPopen)
    return PopenLog


def test_slurm_executor(monkey_popen, tmp_path):
    """
    GIVEN a slurm cluster in a docker container
    WHEN a function is submitted to the cluster executor
    THEN the result is correctly computed
    """
    with FractalSlurmExecutor(script_dir=tmp_path) as executor:
        res = executor.submit(lambda: 42)
    assert res == 42
