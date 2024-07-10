import pytest
from devtools import debug
from pydantic.error_wrappers import ValidationError

from fractal_server.app.schemas.v1 import ManifestV1
from fractal_server.tasks.v1._TaskCollectPip import _TaskCollectPip
from fractal_server.tasks.v1.utils import get_python_interpreter_v1


def test_unit_TaskCollectPip(tmp_path):
    _TaskCollectPip(package="my-package")
    package = tmp_path / "dummy_pkg_1-0.0.1-py3-none-any.whl"
    package.mkdir()
    _TaskCollectPip(package=package.as_posix())
    with pytest.raises(ValidationError):
        _TaskCollectPip(package="somedirectory/my-package")


def test_get_python_interpreter():
    with pytest.raises(ValueError):
        get_python_interpreter_v1(version="1.1")


@pytest.mark.parametrize(
    (
        "package",
        "package_version",
        "package_extras",
        "python_version",
        "expected_source",
    ),
    [
        (
            "my-package",
            "1.2.3",
            None,
            None,
            "pip_remote:my_package:1.2.3::",
        ),
        (
            "my-package",
            "1.2.3",
            "extra1,extra2",
            None,
            "pip_remote:my_package:1.2.3:extra1,extra2:",
        ),
        (
            "my-package",
            "1.2.3",
            "extra1,extra2",
            "3.9",
            "pip_remote:my_package:1.2.3:extra1,extra2:py3.9",
        ),
    ],
)
def test_unit_source_resolution(
    package,
    package_version,
    package_extras,
    python_version,
    expected_source,
):
    """
    GIVEN a task package
    WHEN the source is resolved
    THEN it matches expectations
    """
    args = dict(
        package=package,
        package_name=package,
        package_version=package_version,
    )
    if package_extras:
        args["package_extras"] = package_extras
    if python_version:
        args["python_version"] = python_version
    tc = _TaskCollectPip(**args)
    assert tc.package_source == expected_source


def test_TaskCollectPip_model(dummy_task_package):
    """
    GIVEN a path to a local wheel package
    WHEN it is passed to the `_TaskCollectPip` constructor
    THEN the package name is correctly extracted and the package path
         correctly set
    """
    debug(dummy_task_package)
    tc = _TaskCollectPip(package=dummy_task_package.as_posix())
    debug(tc)

    assert tc.package == "fractal_tasks_dummy"
    assert tc.package_path == dummy_task_package

    # Test multiple cases for the check() method and package_source() property
    with pytest.raises(ValueError):
        tc.check()
    with pytest.raises(ValueError):
        tc.package_source
    tc.package_name = tc.package
    with pytest.raises(ValueError):
        tc.check()
    with pytest.raises(ValueError):
        tc.package_source
    tc.package_version = "1.2.3"
    with pytest.raises(ValueError):
        tc.check()
    debug(tc.package_source)
    tc.package_manifest = ManifestV1(manifest_version="1", task_list=[])
    tc.check()
