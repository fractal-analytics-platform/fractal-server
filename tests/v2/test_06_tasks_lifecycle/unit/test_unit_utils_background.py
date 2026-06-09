import pytest

from fractal_server.exceptions import UnreachableBranchError
from fractal_server.tasks.v2.utils_background import prepare_tasks_metadata


def test_prepare_tasks_metadata_failures():
    with pytest.raises(UnreachableBranchError):
        prepare_tasks_metadata(
            package_manifest=None,
            package_root="",
            python_bin="some",
            project_python_wrapper="some",
            package_version="0",
        )

    with pytest.raises(UnreachableBranchError):
        prepare_tasks_metadata(
            package_manifest=None,
            package_root="",
            package_version="0",
        )
