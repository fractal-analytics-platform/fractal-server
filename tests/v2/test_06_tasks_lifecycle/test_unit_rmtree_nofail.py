import shutil

import pytest

from fractal_server.tasks.v2.local._utils import rmtree_nofail


def test_unit_rmtree_nofail():
    with pytest.raises(FileNotFoundError):
        shutil.rmtree("/non-existing-folder")
    rmtree_nofail(folder_path="/non-existing-folder", logger_name=None)
