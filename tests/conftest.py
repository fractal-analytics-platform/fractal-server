import asyncio
from os import environ
from pathlib import Path

import pytest


environ["PYTHONASYNCIODEBUG"] = "1"


@pytest.fixture(scope="session")
def event_loop():
    _event_loop = asyncio.new_event_loop()
    _event_loop.set_debug(True)
    yield _event_loop


@pytest.fixture(scope="session")
async def testdata_path() -> Path:
    TEST_DIR = Path(__file__).parent
    return TEST_DIR / "data/"


from .fixtures_server import *  # noqa F403
from .fixtures_tasks import *  # noqa F403
