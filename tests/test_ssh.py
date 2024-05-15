import logging
import shlex
import subprocess
import sys
from pathlib import Path

import pytest
from devtools import debug
from fabric.connection import Connection


def is_responsive(container_name):
    try:
        import subprocess

        exec_cmd = ["docker", "ps", "-f", f"name={container_name}"]
        out = subprocess.run(exec_cmd, check=True, capture_output=True)
        if out.stdout.decode("utf-8") is not None:
            return True
    except ConnectionError:
        return False


def _run_ssh_command_in_tests(
    command: str,
    hostname: str,
    username: str,
    password: str,
):

    # hostname = "cluster.s3it.uzh.ch"
    # username = "srv-mls-prbvc"
    # key_path = "/home/ubuntu/.ssh/service_user_sciencecluster.key"
    connection = Connection(
        host=hostname,
        user=username,
        # connect_kwargs={"key_filename": key_path},
        connect_kwargs={"password": password},
    )

    result = connection.run(command, hide=True)
    print("STDOUT:")
    print(result.stdout)
    print("STDERR:")
    print(result.stderr)
    connection.close()

    return result


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


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig, testdata_path: Path):
    logging.critical("docker_compose_file start")

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

    logging.critical("docker_compose_file end")
    return str(testdata_path / "slurm_ssh_docker_images/docker-compose.yml")


def test_ssh(docker_services, docker_compose_project_name, docker_ip):
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
    debug(docker_ip)

    return

    def _run(_cmd: str, stdin_content: str | None = None):
        print("CMD:\n", shlex.split(_cmd))
        proc = subprocess.Popen(
            shlex.split(_cmd),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8",
        )
        if stdin_content is not None:
            print(f"STDIN:\n{stdin_content}")
            # proc.stdin.write(stdin_content)  # FIXME: not working
            stdout, stderr = proc.communicate(
                input=stdin_content
            )  # FIXME not working
        else:
            stdout, stderr = proc.communicate()
        # proc.wait()
        print(f"RETURNCODE:\n{proc.returncode}")
        print(f"STDOUT:\n{stdout}")
        print(f"STDERR:\n{stderr}")
        print()
        return stdout, stderr

    _run(f"docker exec --user root {slurm_container} /usr/sbin/sshd")
    stdout, stderr = _run(
        "docker inspect "
        "-f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "
        f"{slurm_container}"
    )
    ip = stdout.strip()
    debug(ip)
    _run_ssh_command_in_tests(
        command="whoami",
        hostname=ip,
        username="fractal",
        password="fractal",
    )
