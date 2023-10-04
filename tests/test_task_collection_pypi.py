import logging
from pathlib import Path
from typing import Optional

import pytest
from devtools import debug

from .fixtures_tasks import execute_command
from fractal_server.tasks.collection import _init_venv
from fractal_server.tasks.collection import _pip_install
from fractal_server.tasks.collection import _TaskCollectPip
from fractal_server.tasks.collection import create_package_dir_pip
from fractal_server.tasks.collection import download_package
from fractal_server.tasks.collection import inspect_package
from fractal_server.tasks.collection import ManifestV1


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
            "pip_remote:my-package:1.2.3::",
        ),
        (
            "my-package",
            "1.2.3",
            "extra1,extra2",
            None,
            "pip_remote:my-package:1.2.3:extra1,extra2:",
        ),
        (
            "my-package",
            "1.2.3",
            "extra1,extra2",
            "3.9",
            "pip_remote:my-package:1.2.3:extra1,extra2:py3.9",
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


@pytest.mark.parametrize("python_version", [None, "3.10"])
async def test_init_venv(tmp_path, python_version):
    """
    GIVEN a path and a python version
    WHEN _init_venv() is called
    THEN a python venv is initialised at path
    """
    venv_path = tmp_path / "fractal_test"
    venv_path.mkdir(exist_ok=True, parents=True)
    logger_name = "fractal"

    try:
        python_bin = await _init_venv(
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

    await _init_venv(path=venv_path, logger_name=logger_name)
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
    await _init_venv(path=venv_path, logger_name=LOG)

    async def _aux(*, pin: Optional[dict[str, str]] = None) -> str:
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


async def test_inspect_package(tmp_path):
    """
    GIVEN the path to a wheel package
    WHEN the inspect package is called on the path of the wheel
    THEN the name, version and manifest of the package are loaded
    """
    PACKAGE = "fractal-tasks-core==0.9.4"
    task_pkg = _TaskCollectPip(package=PACKAGE)
    pkg_wheel = await download_package(task_pkg=task_pkg, dest=tmp_path)
    debug(pkg_wheel)
    info = inspect_package(pkg_wheel)
    debug(info)
    assert info["pkg_name"] == "fractal_tasks_core"
    assert info["pkg_version"] == "0.9.4"
    assert isinstance(info["pkg_manifest"], ManifestV1)


@pytest.mark.parametrize(
    ("task_pkg", "expected_path"),
    [
        (
            _TaskCollectPip(package="my-package"),
            Path(".fractal/my-package"),
        ),
        (
            _TaskCollectPip(package="my-package", package_version="1.2.3"),
            Path(".fractal/my-package1.2.3"),
        ),
    ],
)
def test_create_pkg_dir(task_pkg, expected_path):
    """
    GIVEN a taks package
    WHEN the directory for installation is created
    THEN the path is the one expected, or we obtain the expected error

    NOTE:
        expected_path relative to FRACTAL_TASKS_DIR
    """
    from fractal_server.config import get_settings

    settings = get_settings()
    check = settings.FRACTAL_TASKS_DIR / expected_path
    if task_pkg.package_version is None:
        with pytest.raises(ValueError):
            venv_path = create_package_dir_pip(task_pkg=task_pkg)
    else:
        venv_path = create_package_dir_pip(task_pkg=task_pkg)
        assert venv_path == check
