from os import environ
from pathlib import Path

import pytest


environ["PYTHONASYNCIODEBUG"] = "1"


def check_basetemp(tpath: Path):
    """
    Check that root temporary directory contains `pytest` in its name.

    This is necessary because some tests use the directory name as a
    discriminant to set and test permissions.
    """
    if "pytest" not in tpath.as_posix():
        raise ValueError(
            f"`basetemp` must contain `pytest` in its name. Got {tpath.parent}"
        )


@pytest.fixture(scope="session")
async def testdata_path() -> Path:
    TEST_DIR = Path(__file__).parent
    return TEST_DIR / "data/"


@pytest.fixture(scope="session")
def tmp777_session_path(tmp_path_factory):
    """
    Makes a subdir of the tmp_path with 777 access rights
    """

    def _tmp_path_factory(relative_path: str):
        tmp = tmp_path_factory.mktemp(relative_path)
        tmp.chmod(0o777)
        check_basetemp(tmp.parent)
        return tmp

    yield _tmp_path_factory


@pytest.fixture
def tmp777_path(tmp_path):
    check_basetemp(tmp_path)
    tmp_path.chmod(0o777)
    for parent in tmp_path.parents:
        if "pytest" in parent.as_posix():
            parent.chmod(0o777)
    yield tmp_path


from .fixtures_server import *  # noqa F403
from .fixtures_server_v1 import *  # noqa F403
from .fixtures_server_v2 import *  # noqa F403
from .fixtures_tasks_v1 import *  # noqa F403
from .fixtures_tasks_v2 import *  # noqa F403
from .fixtures_docker import *  # noqa F403
from .fixtures_slurm import *  # noqa F403
from .fixtures_commands import *  # noqa F403
from .fixtures_tasks_v2_new import *  # noqa F403
