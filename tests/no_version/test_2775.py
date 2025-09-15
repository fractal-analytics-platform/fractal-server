import pytest


@pytest.fixture(scope="session")
async def async_fixture():
    pass


@pytest.fixture
def sync_fixture(async_fixture):
    pass


async def test_2775(
    request,
    async_fixture,  # uncomment to make test pass
):
    print(request.getfixturevalue("sync_fixture"))
