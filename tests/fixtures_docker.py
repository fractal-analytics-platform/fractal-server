import shutil
import sys
from pathlib import Path

import pytest
from pytest_docker.plugin import containers_scope


HAS_LOCAL_SBATCH = bool(shutil.which("sbatch"))


@pytest.fixture(scope=containers_scope)
def docker_cleanup() -> str:
    """
    See
    https://docs.docker.com/compose/faq/#why-do-my-services-take-10-seconds-to-recreate-or-stop.

    docker compose down --help:
       `-t, --timeout int      Specify a shutdown timeout in seconds`
    """
    return ["down -v -t 1"]


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
def docker_compose_file(pytestconfig, testdata_path: Path):
    requirements_file_path = (
        testdata_path / "slurm_docker_images" / "node" / "tmp_requirements.txt"
    )
    _write_requirements_file(requirements_file_path)

    import fractal_server
    import tarfile

    # This same path is hardocded in the Dockerfile of the SLURM node.
    CODE_ROOT = Path(fractal_server.__file__).parent.parent
    TAR_FILE = (
        testdata_path / "slurm_docker_images/node/fractal_server_local.tar.gz"
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
        # in macOS '/tmp' is a symlink to '/private/tmp'
        # if we don't mount '/private', 'mkdir -p /private/...' fails with
        # PermissionDenied
        return str(
            testdata_path / "slurm_docker_images/docker-compose-private.yml"
        )

    return str(testdata_path / "slurm_docker_images/docker-compose.yml")
