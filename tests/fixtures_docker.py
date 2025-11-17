import logging
import os
import shlex
import shutil
import subprocess
import sys
import time
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest
from pytest import TempPathFactory
from pytest_docker.plugin import containers_scope

from fractal_server.ssh._fabric import FractalSSH
from fractal_server.ssh._fabric import FractalSSHList


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
    import tarfile

    import fractal_server

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

    current_python_version_underscore = current_py_version.replace(".", "_")
    if sys.platform == "darwin":
        # in macOS '/tmp' is a symlink to '/private/tmp'
        # if we don't mount '/private', 'mkdir -p /private/...' fails with
        # PermissionDenied
        return str(
            testdata_path
            / "slurm_docker_images"
            / f"docker-compose_{current_python_version_underscore}-private.yml"
        )
    return str(
        testdata_path
        / "slurm_docker_images"
        / f"docker-compose_{current_python_version_underscore}.yml"
    )


@pytest.fixture
def slurmlogin_container(docker_compose_project_name, docker_services) -> str:
    if "DO_NOT_USE_DOCKER" in os.environ:
        raise RuntimeError(
            "You are using 'slurmlogin_container' fixture, but "
            "'DO_NOT_USE_DOCKER' is set: you probably forgot a marker."
        )
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
def run_in_container(slurmlogin_container) -> str:
    def __runner__(cmd: str) -> subprocess.CompletedProcess:
        full_cmd = f"docker exec --user root {slurmlogin_container} {cmd}"
        logging.info(f"Now running {full_cmd=}.")
        res = subprocess.run(
            shlex.split(full_cmd),
            capture_output=True,
            encoding="utf-8",
        )
        return res

    return __runner__


@pytest.fixture
def ssh_alive(slurmlogin_ip, slurmlogin_container) -> None:
    command = (
        f"docker exec --user root {slurmlogin_container} service ssh status"
    )
    max_attempts = 50
    interval = 0.5
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
            f"[ssh_alive] Attempt {attempt + 1}/{max_attempts}, {res.stdout=}"
        )
        logging.info(
            f"[ssh_alive] Attempt {attempt + 1}/{max_attempts}, {res.stderr=}"
        )
        if "sshd is running" in res.stdout:
            logging.info("[ssh_alive] SSH status seems OK, exit.")
            return
        time.sleep(interval)
    raise RuntimeError(f"[ssh_alive] SSH not active on {slurmlogin_container}")


@pytest.fixture
def slurm_alive(slurmlogin_ip, slurmlogin_container) -> None:
    max_attempts = 50
    interval = 0.5
    command = f"docker exec --user root {slurmlogin_container} scontrol ping"
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
            f"[slurm_alive] Attempt {attempt + 1}/{max_attempts}, {res.stdout=}"
        )
        logging.info(
            f"[slurm_alive] Attempt {attempt + 1}/{max_attempts}, {res.stderr=}"
        )

        if "Slurmctld(primary) at slurm is UP" in res.stdout:
            logging.info("[slurm_alive] SLURM status seems OK, exit.")
            return

        time.sleep(interval)
    raise RuntimeError(
        f"[slurm_alive] SLURM not active on {slurmlogin_container}"
    )


@pytest.fixture
def fractal_ssh_list(
    slurmlogin_ip,
    ssh_alive,
    ssh_keys,
) -> Generator[FractalSSHList, Any, None]:
    """
    Return a `FractalSSHList` object which already contains a valid
    `FractalSSH` object.
    """
    collection = FractalSSHList()
    fractal_ssh_obj: FractalSSH = collection.get(
        host=slurmlogin_ip,
        user="fractal",
        key_path=ssh_keys["private"],
    )
    fractal_ssh_obj.check_connection()

    yield collection

    collection.close_all()


@pytest.fixture(scope="session")
def ssh_username() -> str:
    return "test01"


@pytest.fixture
def ssh_config_dict(
    slurmlogin_ip: str,
    ssh_keys: dict[str, str],
    ssh_username: str,
) -> dict[str, str | dict[str, str]]:
    return dict(
        host=slurmlogin_ip,
        user=ssh_username,
        key_path=ssh_keys["private"],
    )


@pytest.fixture
def fractal_ssh(
    fractal_ssh_list,
    slurmlogin_ip,
    ssh_keys,
    ssh_config_dict,
) -> Generator[FractalSSH, Any, None]:
    fractal_ssh_obj: FractalSSH = fractal_ssh_list.get(**ssh_config_dict)
    yield fractal_ssh_obj

    fractal_ssh_obj.close()
