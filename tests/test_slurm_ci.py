import os
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(
        Path().absolute(), "tests/slurm_images", "docker-compose.yml"
    )


def do_nothing():
    pass
