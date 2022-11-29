import json
import os
import shlex
from pathlib import Path
from typing import List

import pytest
from devtools import debug


def is_responsive(container_name):
    try:
        import subprocess

        exec_cmd = ["docker", "ps", "-f", f"name={container_name}"]
        out = subprocess.run(exec_cmd, check=True, capture_output=True)
        if out.stdout.decode("utf-8") is not None:
            return True
    except ConnectionError:
        return False


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(
        Path().absolute(), "tests/slurm_docker_images", "docker-compose.yml"
    )


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
def monkey_slurm(monkeypatch, docker_compose_project_name, docker_services):
    """
    Monkeypatch Popen to execute overridden command in container

    If sbatch is not present on the host machine, check if there is a
    containerised installation and redirect commands there. If no slurm
    container is present, xfail.
    """
    import subprocess

    OrigPopen = subprocess.Popen

    slurm_container = docker_compose_project_name + "_slurm-docker-master_1"

    docker_services.wait_until_responsive(
        timeout=20.0,
        pause=0.5,
        check=lambda: is_responsive(slurm_container),
    )

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

            container_cmd = [" ".join(str(c) for c in cmd)]
            cmd = [
                "docker",
                "exec",
                slurm_container,
                "bash",
                "-c",
            ] + container_cmd
            super().__init__(cmd, *args[1:], **kwargs)
            debug(shlex.join(self.args))
            PopenLog.add_call(self)

    monkeypatch.setattr(subprocess, "Popen", _MockPopen)
    return PopenLog
