import logging
from pathlib import Path

import pytest
from devtools import debug

from fractal_server.app.models.v1 import State
from fractal_server.app.routes.api.v1.task_collection import (
    TaskCollectStatusV1,
)
from fractal_server.tasks.v1._TaskCollectPip import _TaskCollectPip
from fractal_server.tasks.v1.background_operations import _pip_install
from fractal_server.tasks.v1.background_operations import (
    background_collect_pip,
)
from fractal_server.tasks.v1.endpoint_operations import (
    create_package_dir_pip,
)
from fractal_server.tasks.v1.endpoint_operations import download_package
from fractal_server.tasks.v1.endpoint_operations import inspect_package
from fractal_server.tasks.v1.utils import _init_venv_v1
from tests.execute_command import execute_command


@pytest.mark.parametrize("use_current_python", [True, False])
async def test_init_venv(
    tmp_path,
    use_current_python: bool,
    current_py_version: str,
):
    """
    GIVEN a path and a python version
    WHEN _init_venv() is called
    THEN a python venv is initialised at path
    """
    venv_path = tmp_path / "fractal_test"
    venv_path.mkdir(exist_ok=True, parents=True)
    logger_name = "fractal"

    if use_current_python:
        python_version = current_py_version
    else:
        python_version = None

    try:
        python_bin = await _init_venv_v1(
            path=venv_path,
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


async def test_pip_install(tmp_path):
    """
    GIVEN a package name and version and a path with a venv
    WHEN _pip_install() is called
    THEN the package is installed in the venv and the package installation
         location is returned
    """
    PACKAGE = "devtools"
    VERSION = "0.8.0"
    venv_path = tmp_path / "fractal_test" / f"{PACKAGE}{VERSION}"
    venv_path.mkdir(exist_ok=True, parents=True)
    logger_name = "fractal"

    await _init_venv_v1(path=venv_path, logger_name=logger_name)
    location = await _pip_install(
        venv_path=venv_path,
        task_pkg=_TaskCollectPip(package=PACKAGE, package_version=VERSION),
        logger_name=logger_name,
    )
    debug(location)
    assert PACKAGE in location.as_posix()


async def test_pip_install_pinned(tmp_path, caplog):

    caplog.set_level(logging.DEBUG)

    LOG = "fractal_pinned_version"
    PACKAGE = "devtools"
    VERSION = "0.8.0"
    EXTRA = "pygments"
    venv_path = tmp_path / "fractal_test"
    venv_path.mkdir(exist_ok=True, parents=True)
    pip = venv_path / "venv/bin/pip"
    await _init_venv_v1(path=venv_path, logger_name=LOG)

    async def _aux(*, pin: dict[str, str] | None = None) -> str:
        """pip install with pin and return version for EXTRA package"""
        await _pip_install(
            venv_path=venv_path,
            task_pkg=_TaskCollectPip(
                package=PACKAGE,
                package_version=VERSION,
                package_extras=EXTRA,
                pinned_package_versions=pin,
            ),
            logger_name=LOG,
        )
        stdout_inspect = await execute_command(f"{pip} show {EXTRA}")
        extra_version = next(
            line.split()[-1]
            for line in stdout_inspect.split("\n")
            if line.startswith("Version:")
        )
        await execute_command(f"{pip} uninstall {PACKAGE} {EXTRA} -y")
        return extra_version

    # Case 0:
    #   get default EXTRA version and check that it differs from pin version
    #   then try to pin with DEFAULT_VERSION
    DEFAULT_VERSION = await _aux()
    PIN_VERSION = "2.0"
    assert PIN_VERSION != DEFAULT_VERSION
    caplog.clear()

    pin = {EXTRA: DEFAULT_VERSION}
    new_version = await _aux(pin=pin)
    assert new_version == DEFAULT_VERSION
    assert "Specific version required" in caplog.text
    assert "already matches the pinned version" in caplog.text
    caplog.clear()

    # Case 1: good pin
    pin = {EXTRA: PIN_VERSION}
    new_version = await _aux(pin=pin)
    assert new_version == PIN_VERSION
    assert "differs from pinned version" in caplog.text
    assert f"pip install {EXTRA}" in caplog.text
    caplog.clear()

    # Case 2: bad pin with unexisting EXTRA version
    UNEXISTING_EXTRA = "123456789"
    pin = {EXTRA: UNEXISTING_EXTRA}
    with pytest.raises(RuntimeError) as error_info:
        await _aux(pin=pin)
    assert f"pip install {EXTRA}=={UNEXISTING_EXTRA}" in caplog.text
    assert (
        "Could not find a version that satisfies the requirement "
        f"{EXTRA}=={UNEXISTING_EXTRA}"
    ) in str(error_info.value)
    caplog.clear()

    # Case 3: bad pin with not already installed package
    pin = {"pydantic": "1.0.0"}
    with pytest.raises(RuntimeError) as error_info:
        await _aux(pin=pin)
    assert "pip show pydantic" in caplog.text
    assert "Package(s) not found: pydantic" in str(error_info.value)
    caplog.clear()


async def test_download(tmp_path):
    """
    GIVEN a PyPI package name
    WHEN download_package is called
    THEN the package's wheel is download in the destination directory
    """
    PACKAGE = "fractal-tasks-core"
    task_pkg = _TaskCollectPip(package=PACKAGE)
    pkg = await download_package(task_pkg=task_pkg, dest=tmp_path)
    debug(pkg)
    assert pkg.exists()
    assert "whl" in pkg.as_posix()


@pytest.mark.parametrize(
    "relative_wheel_path",
    (
        "dummy_pkg_1/dist/dummy_pkg_1-0.0.1-py3-none-any.whl",
        "dummy_pkg_2/dist/dummy_PKG_2-0.0.1-py3-none-any.whl",
    ),
)
async def test_unit_create_venv_install_package(
    testdata_path: Path,
    tmp_path: Path,
    override_settings_factory: callable,
    relative_wheel_path: str,
):
    """
    This unit test for `_create_venv_install_package` collects tasks from two
    local wheel files.

    ``console
    $ pwd
    /.../fractal-server/tests/data/more_dummy_task_packages
    $ grep name dummy_pkg_*/pyproject.toml | grep -v email
    dummy_pkg_1/pyproject.toml:name = "dummy_pkg_1"
    dummy_pkg_2/pyproject.toml:name = "dummy-PKG-2"
    ```
    """
    from fractal_server.tasks.v1.background_operations import (
        _create_venv_install_package,
    )
    from fractal_server.logger import set_logger

    LOGGER_NAME = "LOGGER"
    set_logger(LOGGER_NAME, log_file_path=(tmp_path / "logs"))

    task_package = (
        testdata_path / "more_dummy_task_packages" / relative_wheel_path
    )
    task_pkg = _TaskCollectPip(package=task_package.as_posix())

    # Extract info form the wheel package (this is part of the endpoint)
    pkg_info = inspect_package(task_pkg.package_path)
    task_pkg.package_name = pkg_info["pkg_name"]
    task_pkg.package_version = pkg_info["pkg_version"]
    task_pkg.package_manifest = pkg_info["pkg_manifest"]
    task_pkg.check()
    debug(task_pkg)

    # Collect task package
    python_bin, package_root = await _create_venv_install_package(
        task_pkg=task_pkg, path=tmp_path, logger_name=LOGGER_NAME
    )
    debug(python_bin)
    debug(package_root)


async def test_logs_failed_collection(
    db, dummy_task_package, override_settings_factory, tmp_path: Path
):
    """
    GIVEN a package and its installation environment
    WHEN the background collection is called on it and it fails
    THEN
        * the log of the collection is saved to the state
        * the installation directory is removed
    """

    override_settings_factory(
        FRACTAL_TASKS_DIR=(tmp_path / "test_logs_failed_collection")
    )

    task_pkg = _TaskCollectPip(package=dummy_task_package.as_posix())

    # Extract info form the wheel package (this is part of the endpoint)
    pkg_info = inspect_package(task_pkg.package_path)
    task_pkg.package_name = pkg_info["pkg_name"]
    task_pkg.package_version = pkg_info["pkg_version"]
    task_pkg.package_manifest = pkg_info["pkg_manifest"]
    task_pkg.check()
    debug(task_pkg)

    venv_path = create_package_dir_pip(task_pkg=task_pkg)
    collection_status = TaskCollectStatusV1(
        status="pending", venv_path=venv_path, package=task_pkg.package
    )
    # replacing with path because of non-serializable Path
    collection_status_dict = collection_status.sanitised_dict()
    state = State(data=collection_status_dict)
    db.add(state)
    await db.commit()
    await db.refresh(state)

    task_pkg.package = "__NO_PACKAGE"
    task_pkg.package_path = None

    await background_collect_pip(
        state_id=state.id,
        venv_path=venv_path,
        task_pkg=task_pkg,
    )

    await db.refresh(state)
    debug(state)
    assert state.data["log"]
    assert state.data["status"] == "fail"
    assert state.data["info"].startswith("Original error")
    assert not venv_path.exists()
