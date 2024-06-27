from pathlib import Path

import pytest
from devtools import debug
from pydantic.error_wrappers import ValidationError

from fractal_server.app.schemas.v2 import ManifestV2
from fractal_server.tasks.utils import _normalize_package_name
from fractal_server.tasks.utils import get_absolute_venv_path
from fractal_server.tasks.v2._TaskCollectPip import _TaskCollectPip
from fractal_server.tasks.v2.utils import get_python_interpreter_v2


def test_unit_TaskCollectPip(tmp_path):
    _TaskCollectPip(package="my-package")
    package = tmp_path / "dummy_pkg_1-0.0.1-py3-none-any.whl"
    package.mkdir()
    _TaskCollectPip(package=package.as_posix())
    with pytest.raises(ValidationError):
        _TaskCollectPip(package="somedirectory/my-package")


def test_get_absolute_venv_path(tmp_path, override_settings_factory):
    FRACTAL_TASKS_DIR = tmp_path / "TASKS"
    override_settings_factory(FRACTAL_TASKS_DIR=FRACTAL_TASKS_DIR)
    absolute_path = tmp_path
    relative_path = Path("somewhere/else/")
    assert get_absolute_venv_path(absolute_path) == absolute_path
    assert get_absolute_venv_path(relative_path) == (
        FRACTAL_TASKS_DIR / relative_path
    )


def test_get_python_interpreter():
    with pytest.raises(ValueError):
        get_python_interpreter_v2(version="1.1")


def test_normalize_package_name():
    """
    Test based on the example in
    https://packaging.python.org/en/latest/specifications/name-normalization.
    """
    inputs = (
        "friendly-bard",
        "Friendly-Bard",
        "FRIENDLY-BARD",
        "friendly.bard",
        "friendly_bard",
        "friendly--bard",
        "FrIeNdLy-._.-bArD",
    )
    outputs = list(map(_normalize_package_name, inputs))
    assert len(set(outputs)) == 1


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
    tc.package_manifest = ManifestV2(manifest_version="2", task_list=[])
    tc.check()
