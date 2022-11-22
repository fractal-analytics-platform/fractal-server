import shlex
from typing import List

import pytest
from devtools import debug


@pytest.fixture
def slurm_container(event_loop) -> str:
    """
    Return the name of the container running `slurm-docker-master`
    """
    import subprocess

    try:
        out = subprocess.run(["docker", "ps"], check=True, capture_output=True)
        output = out.stdout.decode("utf-8")
        slurm_master = next(
            ln for ln in output.splitlines() if "slurm-docker-master" in ln
        )
        container_name = slurm_master.split()[-1]
        debug(container_name)
        return container_name
    except (RuntimeError, StopIteration):
        pytest.xfail(reason="No Slurm master container found")


@pytest.fixture
def monkey_slurm(monkeypatch, request):
    """
    Monkeypatch Popen to execute overridden command in container

    If not present on the host or inserted in the `NO_HOST_CMD` list, intercept
    Popen calls and redirect to the container.
    """
    import subprocess
    import shutil

    OrigPopen = subprocess.Popen

    NO_HOST_CMD = ["sudo"]
    OVERRIDE_CMD = ["sbatch"]
    OVERRIDE_CMD = [c for c in OVERRIDE_CMD if not shutil.which(c)]
    OVERRIDE_CMD.extend(NO_HOST_CMD)

    if OVERRIDE_CMD:
        slurm_container = request.getfixturevalue("slurm_container")

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
