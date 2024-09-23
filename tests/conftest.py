import logging
import sys
import threading
import time
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
    threads = [t for t in threading.enumerate() if not t._is_stopped]
    return threads, len(threads)


@pytest.fixture(scope="function", autouse=True)
def check_threads(request):
    """
    Check that the number of active threads does not increase when running a
    test. When hitting a bad conditiona, the check is repeated several times
    (with a grace-time interval at each iteration) before raising an error.
    This is because it may take some time before some thread is actually
    closed; the maximum test delay is around 2 seconds (plus small overheads).
    """
    test_name = request.node.name
    LOG_PREFIX = f"[check_threads({test_name})]"
    CHECK_THREADS_MAX_ITERATIONS = 10
    CHECK_THREADS_GRACE_TIME = 0.2

    _, num_initial_threads = _get_threads()

    yield

    final_threads, num_final_threads = _get_threads()

    if num_final_threads == num_initial_threads:
        logging.debug(f"{LOG_PREFIX} All good.")
    else:
        logging.warning(
            f"{LOG_PREFIX} " f"{num_final_threads=} != {num_initial_threads=}"
        )
        logging.warning(f"{LOG_PREFIX} {final_threads=}")
        logging.warning(f"{LOG_PREFIX} Start loop of redundant checks")
        for iteration in range(CHECK_THREADS_MAX_ITERATIONS):
            time.sleep(CHECK_THREADS_GRACE_TIME)
            current_threads, num_current_threads = _get_threads()
            logging.warning(f"{LOG_PREFIX} {current_threads=}")
            if num_current_threads == num_initial_threads:
                logging.warning(
                    f"{LOG_PREFIX} At {iteration=}, {num_current_threads=}. "
                    "Break."
                )
                break
        if num_current_threads != num_initial_threads:
            raise RuntimeError(
                f"{LOG_PREFIX} "
                f"After {CHECK_THREADS_MAX_ITERATIONS=}, {current_threads=}"
            )
