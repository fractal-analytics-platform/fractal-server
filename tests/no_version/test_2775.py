import pytest


@pytest.fixture
async def async_fixture():
    yield 99


async def test_2775(
    request,
    async_fixture,  # uncomment to make test pass
):
    print(request.getfixturevalue("async_fixture"))
