import logging
import shlex
import subprocess
import sys
from pathlib import Path

import pytest
from devtools import debug
from fabric.connection import Connection
from pytest_docker.plugin import containers_scope


def is_responsive(container_name):
    try:
        import subprocess

        exec_cmd = ["docker", "ps", "-f", f"name={container_name}"]
        out = subprocess.run(exec_cmd, check=True, capture_output=True)
        if out.stdout.decode("utf-8") is not None:
            return True
    except ConnectionError:
        return False


def _write_requirements_file(path: Path):
    """
    This function creates a temporary requirements file, which is copied
    into the node container and pip-installed from a separate statement.
    For local tests, this improves performance because this layer can be
    cached by Docker. The cache is invalidated whenever some version change.
    """

    import pydantic
    import sqlalchemy
    import fastapi
    import cfut
    import alembic
    import fastapi_users

    with path.open("w") as f:
        f.write(f"pydantic=={pydantic.__version__}\n")
        f.write(f"sqlalchemy=={sqlalchemy.__version__}\n")
        f.write(f"alembic=={alembic.__version__}\n")
        f.write(f"fastapi=={fastapi.__version__}\n")
        f.write(f"fastapi-users=={fastapi_users.__version__}\n")
        f.write(f"clusterfutures=={cfut.__version__}\n")


@pytest.fixture(scope=containers_scope)
def docker_cleanup() -> str:
    """
    See
    https://docs.docker.com/compose/faq/#why-do-my-services-take-10-seconds-to-recreate-or-stop.

    docker compose down --help:
       `-t, --timeout int      Specify a shutdown timeout in seconds`
    """
    return ["down -v --timeout 1"]


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig, testdata_path: Path):
    requirements_file_path = (
        testdata_path
        / "slurm_ssh_docker_images"
        / "node"
        / "tmp_requirements.txt"
    )
    _write_requirements_file(requirements_file_path)

    import fractal_server
    import tarfile

    # This same path is hardocded in the Dockerfile of the SLURM node.
    CODE_ROOT = Path(fractal_server.__file__).parent.parent
    TAR_FILE = (
        testdata_path
        / "slurm_ssh_docker_images/node/fractal_server_local.tar.gz"
    )
    TAR_ROOT = CODE_ROOT.name
    with tarfile.open(TAR_FILE, "w:gz") as tar:
        tar.add(CODE_ROOT, arcname=TAR_ROOT, recursive=False)
        for name in [
            "pyproject.toml",
            "README.md",
            "fractal_server",
        ]:
            f = CODE_ROOT / name
            tar.add(f, arcname=f.relative_to(CODE_ROOT.parent))

    if sys.platform == "darwin":
        raise NotImplementedError()

    return str(testdata_path / "slurm_ssh_docker_images/docker-compose.yml")


########


def _run_locally(_cmd: str, stdin_content: str | None = None):
    print("CMD:\n", shlex.split(_cmd))
    res = subprocess.run(
        shlex.split(_cmd),
        capture_output=True,
        encoding="utf-8",
        check=True,
    )
    print(f"RETURNCODE:\n{res.returncode}")
    print(f"STDOUT:\n{res.stdout}")
    print(f"STDERR:\n{res.stderr}")
    print()
    return res.stdout, res.stderr


def _run_ssh_command_in_tests(
    command: str,
    hostname: str,
    username: str,
    password: str,
):

    print("CMD:\n", shlex.split(command))
    with Connection(
        host=hostname,
        user=username,
        connect_kwargs={"password": password},
    ) as connection:

        res = connection.run(command, hide=True)
        print(f"STDOUT:\n{res.stdout}")
        print(f"STDERR:\n{res.stderr}")

    return res


def test_ssh(docker_services, docker_compose_project_name):
    print(docker_services)

    slurm_container = docker_compose_project_name + "-slurm-docker-master-1"
    logging.warning(f"{docker_compose_project_name=}")
    logging.warning(f"{slurm_container=}")

    docker_services.wait_until_responsive(
        timeout=20.0,
        pause=0.5,
        check=lambda: is_responsive(slurm_container),
    )
    debug(docker_services)

    # Start ssh daemon
    _run_locally(f"docker exec --user root {slurm_container} /usr/sbin/sshd")

    # Get IP
    stdout, stderr = _run_locally(
        "docker inspect "
        "-f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "
        f"{slurm_container}"
    )
    ip = stdout.strip()
    debug(ip)

    # Run hostname through ssh
    stdout, stderr = _run_ssh_command_in_tests(
        command="hostname",
        hostname=ip,
        username="fractal",
        password="fractal",
    )
    assert stdout == "slurm-docker-master"
