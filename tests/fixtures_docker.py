import io
import logging
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
from typing import Generator

import pytest
from fabric.connection import Connection
from pytest import TempPathFactory
from pytest_docker.plugin import containers_scope

from fractal_server.ssh._fabric import FractalSSH


HAS_LOCAL_SBATCH = bool(shutil.which("sbatch"))


@pytest.fixture(scope=containers_scope)
def docker_cleanup() -> str:
    """
    See
    https://github.com/fractal-analytics-platform/fractal-server/pull/1500#issuecomment-2114835978.
    """
    return ["down --volumes --timeout 1"]


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


@pytest.fixture(scope="session")
def ssh_keys(tmp_path_factory: TempPathFactory) -> dict[str, str]:

    folder = tmp_path_factory.mktemp(basename="ssh-keys")
    private_key_path = folder / "testing-ssh-key"
    public_key_path = folder / "testing-ssh-key.pub"

    cmd = f"ssh-keygen -C testing-key -f {private_key_path.as_posix()}  -N ''"
    subprocess.run(
        shlex.split(cmd), capture_output=True, encoding="utf-8", check=True
    )
    key_paths = dict(
        public=public_key_path.as_posix(),
        private=private_key_path.as_posix(),
    )
    return key_paths


@pytest.fixture(scope="session")
def docker_compose_file(
    pytestconfig,
    testdata_path: Path,
    ssh_keys: dict[str, str],
    current_py_version: str,
):

    import fractal_server
    import tarfile

    # Write requirements file
    requirements_file_path = (
        testdata_path / "slurm_docker_images/slurm/tmp_requirements.txt"
    )
    _write_requirements_file(requirements_file_path)

    # Provide a tar.gz archive with fractal-server package
    CODE_ROOT = Path(fractal_server.__file__).parent.parent
    TAR_FILE = (
        testdata_path / "slurm_docker_images/slurm/fractal_server_local.tar.gz"
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

    # Provide a public SSH key
    dest = testdata_path / "slurm_docker_images" / "slurm" / "public_ssh_key"
    shutil.copy(ssh_keys["public"], dest)

    if sys.platform == "darwin":
        # in macOS '/tmp' is a symlink to '/private/tmp'
        # if we don't mount '/private', 'mkdir -p /private/...' fails with
        # PermissionDenied
        return str(
            testdata_path / "slurm_docker_images/docker-compose-private.yml"
        )
    current_python_version_underscore = current_py_version.replace(".", "_")
    return str(
        testdata_path
        / "slurm_docker_images"
        / f"docker-compose_{current_python_version_underscore}.yml"
    )


@pytest.fixture
def slurmlogin_container(docker_compose_project_name, docker_services) -> str:
    logging.warning(f"{docker_compose_project_name=}")
    slurm_container = docker_compose_project_name + "-slurm-1"
    logging.warning(f"{slurm_container=}")
    docker_services.wait_until_responsive(
        timeout=15.0,
        pause=0.5,
        check=lambda: is_responsive(slurm_container),
    )
    return slurm_container


@pytest.fixture
def slurmlogin_ip(slurmlogin_container) -> str:
    cmd = (
        "docker inspect "
        "-f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "
        f"{slurmlogin_container}"
    )
    res = subprocess.run(
        shlex.split(cmd),
        capture_output=True,
        encoding="utf-8",
        check=True,
    )
    ip = res.stdout.strip()
    logging.info(f"{slurmlogin_container=} has {ip=}")
    return ip


@pytest.fixture
def ssh_alive(slurmlogin_ip, slurmlogin_container) -> None:
    command = (
        f"docker exec --user root {slurmlogin_container} service ssh status"
    )
    max_attempts = 10
    interval = 0.2
    logging.info(
        f"Now run {command=} at most {max_attempts} times, "
        f"with a sleep interval of {interval} seconds."
    )
    for attempt in range(max_attempts):
        res = subprocess.run(
            shlex.split(command),
            capture_output=True,
            encoding="utf-8",
        )
        logging.info(
            f"[ssh_alive] Attempt {attempt+1}/{max_attempts}, {res.stdout=}"
        )
        logging.info(
            f"[ssh_alive] Attempt {attempt+1}/{max_attempts}, {res.stderr=}"
        )
        if "sshd is running" in res.stdout:
            logging.info("[ssh_alive] SSH status seems OK, exit.")
            return
        time.sleep(interval)
    raise RuntimeError(f"[ssh_alive] SSH not active on {slurmlogin_container}")


@pytest.fixture
def fractal_ssh(
    slurmlogin_ip,
    ssh_alive,
    ssh_keys,
    monkeypatch,
) -> Generator[FractalSSH, Any, None]:
    ssh_private_key = ssh_keys["private"]

    # https://github.com/fabric/fabric/issues/1979
    # https://github.com/fabric/fabric/issues/2005#issuecomment-525664468
    monkeypatch.setattr("sys.stdin", io.StringIO(""))

    with Connection(
        host=slurmlogin_ip,
        user="fractal",
        forward_agent=False,
        connect_kwargs={"key_filename": ssh_private_key},
    ) as connection:
        fractal_conn = FractalSSH(connection=connection)
        fractal_conn.check_connection()
        yield fractal_conn
