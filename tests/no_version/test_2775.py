import pytest


@pytest.fixture
def intermediate(testdata_path):
    pass


@pytest.fixture
def syncfixture(intermediate):
    yield "This is the sync fixture"


@pytest.mark.container
async def test_2775(
    request,
    # testdata_path,   # CHANGE THIS TO CHANGE BEHAVIOR
):
    print(request.getfixturevalue("syncfixture"))
