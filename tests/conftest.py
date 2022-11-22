import asyncio
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def event_loop():
    _event_loop = asyncio.new_event_loop()
    yield _event_loop


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
        return tmp

    yield _tmp_path_factory


@pytest.fixture
def tmp777_path(tmp_path):
    tmp_path.chmod(0o777)
    yield tmp_path


from .fixtures_server import *  # noqa F403
from .fixtures_tasks import *  # noqa F403
from .fixtures_slurm import *  # noqa F403
