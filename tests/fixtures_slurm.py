import pytest


@pytest.fixture
def ssh_params():
    return dict(
        hostname="localhost", username="test0", password="pwd_test0",
        port=10022
    )
