import pytest
from devtools import debug
from pydantic.error_wrappers import ValidationError

from fractal_server.app.schemas.v2 import ManifestV2
from fractal_server.config import get_settings
from fractal_server.syringe import Inject
from fractal_server.tasks.v2._TaskCollectPip import _TaskCollectPip


def test_TaskCollectPip_model(tmp_path, dummy_task_package):

    settings = Inject(get_settings)
    PYTHON_VERSION = settings.FRACTAL_TASKS_PYTHON_DEFAULT_VERSION

    # package_name is set correctly, for remote package
    task_pkg = _TaskCollectPip(
        package="my-package", python_version=PYTHON_VERSION
    )
    assert task_pkg.package_name == "my-package"

    # package_name is set correctly (and normalized), for remote package
    task_pkg = _TaskCollectPip(
        package="my____PACKAGE", python_version=PYTHON_VERSION
    )
    debug(task_pkg)
    assert task_pkg.package_name == "my-package"

    # package_name and package_path are set correctly, for local wheelfile
    wheel_path = tmp_path / "dummy_pkg_xy-0.0.1-py3-none-any.whl"
    wheel_path.touch()
    task_pkg = _TaskCollectPip(
        package=wheel_path.as_posix(), python_version=PYTHON_VERSION
    )
    assert task_pkg.package_name == "dummy-pkg-xy"
    assert task_pkg.package_path == wheel_path

    # failure for non-absolute wheel path
    with pytest.raises(ValidationError) as e:
        _TaskCollectPip(
            package="non-absolute/path", python_version=PYTHON_VERSION
        )
    assert "must be absolute" in str(e.value)

    # failure for package name ending in .whl
    with pytest.raises(ValidationError) as e:
        _TaskCollectPip(package="x.whl", python_version=PYTHON_VERSION)
    assert "is not the absolute path to a wheel file" in str(e.value)

    # When `package_version` is unset, some methods should fail
    tc = _TaskCollectPip(package="my-package", python_version=PYTHON_VERSION)
    assert tc.package_version is None
    with pytest.raises(ValueError):
        tc.check()
    with pytest.raises(ValueError):
        tc.package_source

    # After setting `package_version`, the `package_source` property works but
    # the `check` method does not
    tc.package_version = "1.2.3"
    debug(tc.package_source)
    with pytest.raises(ValueError):
        tc.check()

    # After setting `package_manifest`, also `check()` works
    tc.package_manifest = ManifestV2(manifest_version="2", task_list=[])
    tc.check()


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
            "3.12",
            "pip_remote:my_package:1.2.3::py3.12",
        ),
        (
            "my-package",
            "1.2.3",
            "extra1,extra2",
            "3.12",
            "pip_remote:my_package:1.2.3:extra1,extra2:py3.12",
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
    settings = Inject(get_settings)
    PYTHON_VERSION = settings.FRACTAL_TASKS_PYTHON_DEFAULT_VERSION
    args = dict(
        package=package,
        package_name=package,
        package_version=package_version,
        python_version=PYTHON_VERSION,
    )
    if package_extras:
        args["package_extras"] = package_extras
    if python_version:
        args["python_version"] = python_version
    tc = _TaskCollectPip(**args)
    assert tc.package_source == expected_source
