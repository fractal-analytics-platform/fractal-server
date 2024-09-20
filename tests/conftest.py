import sys
import threading
import time
from os import environ
from pathlib import Path

import pytest
from devtools import debug


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


@pytest.fixture(scope="session")
def current_py_version() -> str:
    INFO = sys.version_info
    return f"{INFO.major}.{INFO.minor}"


from .fixtures_server import *  # noqa F403
from .fixtures_server_v1 import *  # noqa F403
from .fixtures_server_v2 import *  # noqa F403
from .fixtures_tasks_v1 import *  # noqa F403
from .fixtures_tasks_v2 import *  # noqa F403
from .fixtures_docker import *  # noqa F403
from .fixtures_slurm import *  # noqa F403
from .fixtures_commands import *  # noqa F403


def _get_threads():
    # threads = threading.enumerate()
    threads = [t for t in threading.enumerate() if not t._is_stopped]
    return threads


@pytest.fixture(scope="function", autouse=True)
def count_threads():
    initial_threads = _get_threads()
    yield
    final_threads = _get_threads()

    # Grace time, before error
    if len(final_threads) != len(initial_threads):
        time.sleep(0.5)

    final_threads = _get_threads()
    if len(final_threads) != len(initial_threads):
        debug(initial_threads)
        debug(final_threads)
        raise RuntimeError(f"{initial_threads=}, {final_threads=}")
