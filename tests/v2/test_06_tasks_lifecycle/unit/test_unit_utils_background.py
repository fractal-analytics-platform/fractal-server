import pytest

from fractal_server.exceptions import UnreachableBranchError
from fractal_server.tasks.v2.utils_background import prepare_tasks_metadata


def test_prepare_tasks_metadata_failures():
    with pytest.raises(UnreachableBranchError):
        prepare_tasks_metadata(
            package_manifest=None,
            package_root="",
            python_bin="some",
            pixi_bin="some",
        )

    with pytest.raises(UnreachableBranchError):
        prepare_tasks_metadata(
            package_manifest=None,
            package_root="",
        )

    with pytest.raises(UnreachableBranchError):
        prepare_tasks_metadata(
            package_manifest=None,
            package_root="",
            pixi_bin="some",
        )
