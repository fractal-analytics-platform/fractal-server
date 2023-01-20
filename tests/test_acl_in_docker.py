import logging
import shlex
import subprocess

import pytest
from devtools import debug

from .fixtures_slurm import is_responsive


def run_as_user_on_docker(*, user: str, cmd, container: str):
    if type(cmd) != str:
        cmd = shlex.join(cmd)
    docker_cmd = [
        "docker",
        "exec",
        "--user",
        user,
        container,
        "bash",
        "-c",
    ] + [cmd]

    debug(shlex.join(docker_cmd))
    res = subprocess.run(docker_cmd, encoding="utf-8", capture_output=True)
    debug(res)
    return res


@pytest.fixture
def docker_ready(docker_compose_project_name, docker_services):
    slurm_container = docker_compose_project_name + "_slurm-docker-master_1"
    logging.info(f"{docker_compose_project_name=}")
    logging.info(f"{slurm_container=}")
    docker_services.wait_until_responsive(
        timeout=20.0,
        pause=0.5,
        check=lambda: is_responsive(slurm_container),
    )
    logging.info("docker_services responsive")
    return slurm_container


def test_unit_docker_commands(docker_ready):

    # Check that we can run simmple commands
    for user in ["fractal", "test01", "test02"]:
        res = run_as_user_on_docker(
            user=user, cmd="whoami", container=docker_ready
        )
        assert user in res.stdout

    # Check that we can set umask
    for user in ["fractal", "test01", "test02"]:
        res = run_as_user_on_docker(
            user=user, cmd="umask", container=docker_ready
        )
        print(res.stdout)


def test_acl_permissions(docker_ready, tmp777_path):
    folder = str(tmp777_path / "workflow_dir")
    debug(folder)

    res = run_as_user_on_docker(
        user="fractal", cmd=f"mkdir {folder}", container=docker_ready
    )
    res = run_as_user_on_docker(
        user="fractal", cmd=f"ls -la {folder}", container=docker_ready
    )
    print(res.stdout)

    res = run_as_user_on_docker(
        user="fractal", cmd=f"chmod 700 {folder}", container=docker_ready
    )
    res = run_as_user_on_docker(
        user="fractal", cmd=f"ls -la {folder}", container=docker_ready
    )
    print(res.stdout)

    # ACL
    acl_commands = [
        f"setfacl -b {folder}",
        f"setfacl --modify user:fractal:rwx,user:fractal:rwx,group::---,other::--- {folder}",  # noqa: E501
        f"setfacl --modify user:test01:rwx,user:test01:rwx,group::---,other::--- {folder}",  # noqa: E501
        f"getfacl -p {folder}",
    ]

    for cmd in acl_commands:
        res = run_as_user_on_docker(
            user="fractal", cmd=cmd, container=docker_ready
        )
        assert res.returncode == 0

    # Create as fractal, read as test01, fail reading as test02
    res = run_as_user_on_docker(
        user="fractal", cmd=f"touch {folder}/file-a", container=docker_ready
    )
    assert res.returncode == 0
    res = run_as_user_on_docker(
        user="test01", cmd=f"cat {folder}/file-a", container=docker_ready
    )
    assert res.returncode == 0
    res = run_as_user_on_docker(
        user="test02", cmd=f"cat {folder}/file-a", container=docker_ready
    )
    assert "Permission denied" in res.stderr
    assert res.returncode != 0

    # Create as test01, read as fractal, fail reading as test02
    res = run_as_user_on_docker(
        user="test01", cmd=f"touch {folder}/file-b", container=docker_ready
    )
    assert res.returncode == 0
    res = run_as_user_on_docker(
        user="fractal", cmd=f"cat {folder}/file-b", container=docker_ready
    )
    assert res.returncode == 0
    res = run_as_user_on_docker(
        user="test02", cmd=f"cat {folder}/file-b", container=docker_ready
    )
    assert "Permission denied" in res.stderr
    assert res.returncode != 0

    # Fail creating as test02
    res = run_as_user_on_docker(
        user="test02", cmd=f"touch {folder}/file-c", container=docker_ready
    )
    assert "Permission denied" in res.stderr
    assert res.returncode != 0

    # Cleanup (to make garbage collection easier)
    res = run_as_user_on_docker(
        user="fractal", cmd=f"chmod 777 {folder}", container=docker_ready
    )
