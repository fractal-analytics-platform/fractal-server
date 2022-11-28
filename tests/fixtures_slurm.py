import json
import shlex
from typing import List

import pytest
from devtools import debug


@pytest.fixture(scope="session")
def slurm_config(override_settings):
    config = {
        "default": dict(partition="main", mem="1024"),
        "low": dict(partition="main", mem="128"),
        "cpu-low": dict(partition="main"),
    }

    with override_settings.FRACTAL_SLURM_CONFIG_FILE.open("w") as f:
        json.dump(config, f)
    return config


@pytest.fixture
def slurm_container(event_loop) -> str:
    """
    Return the name of the container running `*slurmmaster`
    """
    import subprocess

    try:
        out = subprocess.run(["docker", "ps"], check=True, capture_output=True)
        output = out.stdout.decode("utf-8")
        slurm_master = next(
            ln for ln in output.splitlines() if "slurmmaster" in ln
        )
        container_name = slurm_master.split()[-1]
        debug(container_name)
        return container_name
    except (RuntimeError, StopIteration):
        pytest.xfail(reason="No Slurm master container found")
    except FileNotFoundError:
        pytest.xfail(reason="Docker not found on host")


@pytest.fixture
def monkey_slurm(monkeypatch, request):
    """
    Monkeypatch Popen to execute overridden command in container

    If sbatch is not present on the host machine, check if there is a
    containerised installation and redirect commands there. If no slurm
    container is present, xfail.
    """
    import subprocess
    import shutil

    OrigPopen = subprocess.Popen

    if not shutil.which("sbatch"):
        OVERRIDE_CMD = ["sudo", "sbatch"]
        slurm_container = request.getfixturevalue("slurm_container")
    else:
        OVERRIDE_CMD = []

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
            assert isinstance(cmd, list)

            if cmd[0] in OVERRIDE_CMD:
                cmd = ["docker", "exec", slurm_container] + cmd
            super().__init__(cmd, *args[1:], **kwargs)
            debug(shlex.join(self.args))
            PopenLog.add_call(self)

    monkeypatch.setattr(subprocess, "Popen", _MockPopen)
    return PopenLog
