import pytest
from devtools import debug
from pydantic.error_wrappers import ValidationError

from fractal_server.common.schemas import TaskCollectPip


def test_TaskCollectPip():
    # Successful creation
    c = TaskCollectPip(package="some-package")
    debug(c)
    assert c
    c = TaskCollectPip(package="/some/package.whl")
    debug(c)
    assert c
    # Failed creation
    with pytest.raises(ValidationError):
        c = TaskCollectPip(package="some/package")
    with pytest.raises(ValidationError):
        c = TaskCollectPip(package="/some/package.tar.gz")
    with pytest.raises(ValidationError):
        c = TaskCollectPip(package="some-package", package_extras="")
    with pytest.raises(ValidationError):
        c = TaskCollectPip(package="some-package", package_extras=None)

    c = TaskCollectPip(package="some-package", pinned_package_versions={})
    with pytest.raises(ValidationError):
        c = TaskCollectPip(package="some-package", pinned_package_versions=1)
