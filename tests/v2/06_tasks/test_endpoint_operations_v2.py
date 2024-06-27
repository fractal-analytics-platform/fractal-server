from pathlib import Path
from typing import Optional

import pytest
from devtools import debug

from fractal_server.app.schemas.v2 import ManifestV2
from fractal_server.config import get_settings
from fractal_server.syringe import Inject
from fractal_server.tasks.v2._TaskCollectPip import _TaskCollectPip
from fractal_server.tasks.v2.endpoint_operations import create_package_dir_pip
from fractal_server.tasks.v2.endpoint_operations import download_package
from fractal_server.tasks.v2.endpoint_operations import inspect_package


def _get_task_pkg(
    *,
    package: str,
    package_version: Optional[str] = None,
    python_version: Optional[str] = None,
) -> _TaskCollectPip:
    if python_version is None:
        settings = Inject(get_settings)
        python_version = settings.FRACTAL_TASKS_PYTHON_DEFAULT_VERSION
    attributes = dict(package=package, python_version=python_version)
    if package_version is not None:
        attributes["package_version"] = package_version
    return _TaskCollectPip(**attributes)


async def test_download_package(tmp_path: Path):
    # Package version is set
    PACKAGE_VERSION = "1.0.1"
    task_pkg = _get_task_pkg(
        package="fractal-tasks-core", package_version=PACKAGE_VERSION
    )
    wheel_path = await download_package(task_pkg=task_pkg, dest=tmp_path)
    debug(wheel_path)
    assert wheel_path.exists()
    assert PACKAGE_VERSION in wheel_path.name

    # Package version is not set
    task_pkg = _get_task_pkg(
        package="fractal-tasks-core", package_version=None
    )
    wheel_path = await download_package(task_pkg=task_pkg, dest=tmp_path)
    debug(wheel_path)
    assert wheel_path.exists()


async def test_inspect_package(tmp_path: Path):
    """
    This also covers all branches of `_load_manifest_from_wheel`.
    """

    # Failure: input is not a wheel file
    with pytest.raises(ValueError) as e:
        inspect_package(path=Path("/something/invalid"))
    assert "Only wheel packages are supported" in str(e)

    # Failure: package has no manifest
    task_pkg = _get_task_pkg(package="devtools")
    wheel_path = await download_package(task_pkg=task_pkg, dest=tmp_path)
    with pytest.raises(ValueError) as e:
        inspect_package(path=wheel_path)
    assert "does not include __FRACTAL_MANIFEST__.json" in str(e.value)

    # Failure: Package has V1 manifest
    task_pkg = _get_task_pkg(
        package="fractal-tasks-core", package_version="0.14.0"
    )
    wheel_path = await download_package(task_pkg=task_pkg, dest=tmp_path)
    with pytest.raises(ValueError) as e:
        inspect_package(path=wheel_path)
    assert "Manifest version manifest_version='1' not supported" in str(
        e.value
    )

    # Success
    PKG_NAME = "fractal-tasks-core"
    PKG_VERSION = "1.0.1"
    task_pkg = _get_task_pkg(package=PKG_NAME, package_version=PKG_VERSION)
    wheel_path = await download_package(task_pkg=task_pkg, dest=tmp_path)
    info = inspect_package(path=wheel_path)
    assert info["pkg_name"] == PKG_NAME
    assert info["pkg_version"] == PKG_VERSION
    assert isinstance(info["pkg_manifest"], ManifestV2)


def test_create_package_dir_pip():
    # Failure because of no version
    task_pkg = _get_task_pkg(package="whatever")
    with pytest.raises(ValueError) as e:
        venv_path = create_package_dir_pip(task_pkg=task_pkg)
    assert "version=None" in str(e)
    # Success
    task_pkg = _get_task_pkg(package="whatever", package_version="1.2.3")
    venv_path = create_package_dir_pip(task_pkg=task_pkg)
    settings = Inject(get_settings)
    expected_venv_path = settings.FRACTAL_TASKS_DIR / ".fractal/whatever1.2.3"
    debug(venv_path)
    debug(expected_venv_path)
    assert venv_path == expected_venv_path
