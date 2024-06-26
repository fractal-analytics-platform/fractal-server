from pathlib import Path

import pytest
from devtools import debug

from fractal_server.config import get_settings
from fractal_server.syringe import Inject
from fractal_server.tasks.endpoint_operations import create_package_dir_pip
from fractal_server.tasks.endpoint_operations import download_package
from fractal_server.tasks.endpoint_operations import inspect_package
from fractal_server.tasks.v2._TaskCollectPip import _TaskCollectPip


async def test_inspect_package(tmp_path):
    """
    GIVEN the path to a wheel package
    WHEN the inspect package is called on the path of the wheel
    THEN the name, version and manifest of the package are loaded
    """
    PACKAGE = "fractal-tasks-core==0.14.3"
    task_pkg = _TaskCollectPip(package=PACKAGE)
    pkg_wheel = await download_package(task_pkg=task_pkg, dest=tmp_path)
    debug(pkg_wheel)
    info = inspect_package(pkg_wheel)
    debug(info)
    assert info["pkg_name"] == "fractal-tasks-core"
    assert info["pkg_version"] == "0.14.3"
    assert info["pkg_manifest"] is not None


async def test_inspect_package_fail(tmp_path):
    wheel_path = tmp_path
    with pytest.raises(ValueError):
        inspect_package(wheel_path)


@pytest.mark.parametrize(
    ("task_pkg_dict", "expected_path"),
    [
        (
            dict(package="my-package"),
            Path(".fractal/my-package"),
        ),
        (
            dict(package="my-package", package_version="1.2.3"),
            Path(".fractal/my-package1.2.3"),
        ),
    ],
)
def test_create_package_dir_pip(task_pkg_dict, expected_path):
    """
    GIVEN a taks package
    WHEN the directory for installation is created
    THEN the path is the one expected, or we obtain the expected error

    NOTE:
        expected_path relative to FRACTAL_TASKS_DIR
    """

    if "python_version" not in task_pkg_dict.keys():
        settings = Inject(get_settings)
        task_pkg_dict[
            "python_version"
        ] = settings.FRACTAL_TASKS_PYTHON_DEFAULT_VERSION

    task_pkg = _TaskCollectPip(**task_pkg_dict)

    settings = Inject(get_settings)
    check = settings.FRACTAL_TASKS_DIR / expected_path
    if task_pkg.package_version is None:
        with pytest.raises(ValueError):
            venv_path = create_package_dir_pip(task_pkg=task_pkg)
    else:
        venv_path = create_package_dir_pip(task_pkg=task_pkg)
        assert venv_path == check
