import logging
from pathlib import Path
from typing import Optional

import pytest
from devtools import debug

from fractal_server.config import get_settings
from fractal_server.syringe import Inject
from fractal_server.tasks.utils import COLLECTION_FREEZE_FILENAME
from fractal_server.tasks.v2._TaskCollectPip import _TaskCollectPip
from fractal_server.tasks.v2._venv_pip import (
    _create_venv_install_package_pip,
)
from fractal_server.tasks.v2._venv_pip import _init_venv_v2
from fractal_server.tasks.v2._venv_pip import _pip_install
from fractal_server.tasks.v2.endpoint_operations import inspect_package
from tests.execute_command import execute_command

LOGGER_NAME = "__logger__"


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


@pytest.mark.parametrize("local_or_remote", ("local", "remote"))
async def test_pip_install(local_or_remote, tmp_path, testdata_path):
    settings = Inject(get_settings)
    PYTHON_VERSION = settings.FRACTAL_TASKS_PYTHON_DEFAULT_VERSION

    # Prepare package
    if local_or_remote == "local":
        PACKAGE = (
            testdata_path
            / "../v2/fractal_tasks_mock/dist"
            / "fractal_tasks_mock-0.0.1-py3-none-any.whl"
        ).as_posix()
        task_pkg = _get_task_pkg(
            package=PACKAGE,
            python_version=PYTHON_VERSION,
        )
    else:
        PACKAGE = "devtools"
        VERSION = "0.8.0"
        task_pkg = _get_task_pkg(
            package=PACKAGE,
            package_version=VERSION,
            python_version=PYTHON_VERSION,
        )

    # Prepare venv
    venv_path = tmp_path / "pkg_folder"
    venv_path.mkdir(exist_ok=True, parents=True)
    await _init_venv_v2(
        venv_path=venv_path,
        python_version=PYTHON_VERSION,
        logger_name=LOGGER_NAME,
    )

    # Pip install
    debug(task_pkg)
    location = await _pip_install(
        venv_path=venv_path,
        task_pkg_to_deprecate=task_pkg,
        logger_name=LOGGER_NAME,
    )
    assert location.exists()

    # Check freeze file
    freeze_file = venv_path / COLLECTION_FREEZE_FILENAME
    assert freeze_file.exists()
    with freeze_file.open("r") as f:
        freeze_data = f.read()
    assert task_pkg.package_name in freeze_data


async def test_pip_install_pinned(tmp_path, caplog):

    caplog.set_level(logging.DEBUG)

    settings = Inject(get_settings)
    PYTHON_VERSION = settings.FRACTAL_TASKS_PYTHON_DEFAULT_VERSION

    LOG = "fractal_pinned_version"
    PACKAGE = "devtools"
    VERSION = "0.8.0"
    EXTRA = "pygments"
    venv_path = tmp_path / "fractal_test"
    venv_path.mkdir(exist_ok=True, parents=True)
    pip = venv_path / "venv/bin/pip"
    await _init_venv_v2(
        venv_path=venv_path, logger_name=LOG, python_version=PYTHON_VERSION
    )

    async def _aux(*, pin: Optional[dict[str, str]] = None) -> str:
        """pip install with pin and return version for EXTRA package"""
        # Pip install
        if pin is None:
            pin = {}
        await _pip_install(
            venv_path=venv_path,
            task_pkg_to_deprecate=_TaskCollectPip(
                package=PACKAGE,
                package_version=VERSION,
                package_extras=EXTRA,
                pinned_package_versions=pin,
                python_version=PYTHON_VERSION,
            ),
            logger_name=LOG,
        )
        # Find version of EXTRA in pip-freeze output
        with (venv_path / COLLECTION_FREEZE_FILENAME).open("r") as f:
            freeze_data = f.read().splitlines()
        extra_version = next(
            line.split("==")[1]
            for line in freeze_data
            if line.lower().startswith(EXTRA.lower())
        )
        debug(extra_version)
        # Clean up
        await execute_command(f"{pip} uninstall {PACKAGE} {EXTRA} -y")
        return extra_version

    # Case 0:
    #   get default version of EXTRA, and then use it as a pin
    DEFAULT_VERSION = await _aux()
    caplog.clear()
    pin = {EXTRA: DEFAULT_VERSION}
    new_version = await _aux(pin=pin)
    assert new_version == DEFAULT_VERSION
    assert "Specific version required" in caplog.text
    assert "already matches the pinned version" in caplog.text
    caplog.clear()

    # Case 1: pin EXTRA to a specific (non-default) version
    PIN_VERSION = "2.0"
    assert PIN_VERSION != DEFAULT_VERSION
    pin = {EXTRA: PIN_VERSION}
    new_version = await _aux(pin=pin)
    assert new_version == PIN_VERSION
    assert "differs from pinned version" in caplog.text
    assert f"pip install {EXTRA}" in caplog.text
    caplog.clear()

    # Case 2: bad pin with invalid EXTRA version
    INVALID_EXTRA_VERSION = "123456789"
    pin = {EXTRA: INVALID_EXTRA_VERSION}
    with pytest.raises(RuntimeError) as error_info:
        await _aux(pin=pin)
    assert f"pip install {EXTRA}=={INVALID_EXTRA_VERSION}" in caplog.text
    assert (
        "Could not find a version that satisfies the requirement "
        f"{EXTRA}=={INVALID_EXTRA_VERSION}"
    ) in str(error_info.value)
    caplog.clear()

    # Case 3: bad pin with package which was not already installed
    pin = {"pydantic": "1.0.0"}
    with pytest.raises(RuntimeError) as error_info:
        await _aux(pin=pin)
    assert "pip show pydantic" in caplog.text
    assert "Package(s) not found: pydantic" in str(error_info.value)
    caplog.clear()


@pytest.mark.parametrize("use_current_python", [True, False])
async def test_init_venv(
    tmp_path,
    use_current_python,
    current_py_version: str,
):
    """
    GIVEN a path and a python version
    WHEN _init_venv_v2() is called
    THEN a python venv is initialised at path
    """
    venv_path = tmp_path / "fractal_test"
    venv_path.mkdir(exist_ok=True, parents=True)
    logger_name = "fractal"

    python_version = current_py_version if use_current_python else None

    try:
        python_bin = await _init_venv_v2(
            venv_path=venv_path,
            logger_name=logger_name,
            python_version=python_version,
        )
    except ValueError as e:
        pytest.xfail(reason=str(e))

    assert venv_path.exists()
    assert (venv_path / "venv").exists()
    assert (venv_path / "venv/bin/python").exists()
    assert (venv_path / "venv/bin/pip").exists()
    assert python_bin.exists()
    assert python_bin == venv_path / "venv/bin/python"
    if python_version:
        version = await execute_command(f"{python_bin} --version")
        assert python_version in version


async def test_create_venv_install_package_pip(
    testdata_path: Path,
    tmp_path: Path,
):
    """
    This unit test for `_create_venv_install_package` collects tasks from a
    local wheel file.
    """

    from fractal_server.logger import set_logger

    LOGGER_NAME = "LOGGER"
    set_logger(LOGGER_NAME, log_file_path=(tmp_path / "logs"))

    task_package = (
        testdata_path
        / "../v2/fractal_tasks_mock/dist"
        / "fractal_tasks_mock-0.0.1-py3-none-any.whl"
    )

    settings = Inject(get_settings)
    PYTHON_VERSION = settings.FRACTAL_TASKS_PYTHON_DEFAULT_VERSION
    task_pkg = _TaskCollectPip(
        package=task_package.as_posix(), python_version=PYTHON_VERSION
    )

    # Extract info form the wheel package (this is part of the endpoint)
    pkg_info = inspect_package(task_pkg.package_path)
    task_pkg.package_version = pkg_info["pkg_version"]
    task_pkg.package_manifest = pkg_info["pkg_manifest"]
    task_pkg.check()
    debug(task_pkg)

    # Collect task package
    python_bin, package_root = await _create_venv_install_package_pip(
        task_pkg_to_deprecate=task_pkg,
        venv_path=tmp_path,
        logger_name=LOGGER_NAME,
    )
    debug(python_bin)
    debug(package_root)
