import os
from pathlib import Path

import pytest
from requests.exceptions import ConnectionError


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(
        Path().absolute(), "tests/slurm_docker_images", "docker-compose.yml"
    )


def is_responsive(container_name):
    try:
        import subprocess

        exec_cmd = ["docker", "ps", "-f", f"name={container_name}"]
        out = subprocess.run(exec_cmd, check=True, capture_output=True)
        if out.stdout.decode("utf-8") is not None:
            return True
    except ConnectionError:
        return False


def test_status_code(docker_services, docker_compose_project_name):
    docker_services.wait_until_responsive(
        timeout=20.0,
        pause=0.5,
        check=lambda: is_responsive(
            docker_compose_project_name + "_slurm-docker-master_1"
        ),
    )
    import subprocess

    exec_cmd = [
        "docker",
        "exec",
        docker_compose_project_name + "_slurm-docker-master_1",
        "bash",
        "-c",
    ]
    sbatch_cmd = 'sbatch --parsable --wrap "pwd"'
    exec_cmd.append(sbatch_cmd)
    out = subprocess.run(exec_cmd, check=True, capture_output=True)

    assert (
        int(out.stdout.decode("utf-8")) == 2
    )  # first job submit has always ID 2
