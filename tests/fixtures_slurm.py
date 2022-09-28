from pathlib import Path

import pytest


@pytest.fixture
def ssh_params():
    # Make sure that the share for user `test0` exists
    # share = Path("/tmp/slurm_share/test0/")
    # share.mkdir(mode=0o777, exist_ok=True)
    return dict(
        hostname="localhost",
        username="test0",
        password="pwd_test0",
        port=10022,
    )


@pytest.fixture(scope="session")
def slurm_shared_tmp_path_factory(tmp_path_factory):
    basepath = Path("/tmp/slurm_share/")
    basepath.mkdir(mode=0o777, exist_ok=True)
    tmp_path_factory._basetemp = basepath
    return tmp_path_factory


@pytest.fixture
def slurm_shared_path(slurm_shared_tmp_path_factory):
    tmp = slurm_shared_tmp_path_factory.mktemp("data")
    tmp.chmod(0o777)
    return tmp
