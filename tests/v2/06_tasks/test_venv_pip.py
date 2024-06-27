import logging
from typing import Optional

import pytest
from devtools import debug

from fractal_server.config import get_settings
from fractal_server.syringe import Inject
from fractal_server.tasks.v2._TaskCollectPip import _TaskCollectPip
from fractal_server.tasks.v2._venv_pip import _init_venv_v2
from fractal_server.tasks.v2._venv_pip import _pip_install
from tests.execute_command import execute_command


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
    settings = Inject(get_settings)
    settings.check_tasks_python()
    python_version = settings.FRACTAL_TASKS_PYTHON_DEFAULT_VERSION

    await _init_venv_v2(
        path=venv_path, python_version=python_version, logger_name=logger_name
    )
    location = await _pip_install(
        venv_path=venv_path,
        task_pkg=_TaskCollectPip(
            package=PACKAGE,
            package_version=VERSION,
            python_version=python_version,
        ),
        logger_name=logger_name,
    )
    debug(location)
    assert PACKAGE in location.as_posix()


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
        path=venv_path, logger_name=LOG, python_version=PYTHON_VERSION
    )

    async def _aux(*, pin: Optional[dict[str, str]] = None) -> str:
        """pip install with pin and return version for EXTRA package"""
        await _pip_install(
            venv_path=venv_path,
            task_pkg=_TaskCollectPip(
                package=PACKAGE,
                package_version=VERSION,
                package_extras=EXTRA,
                pinned_package_versions=pin,
                python_version=PYTHON_VERSION,
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


@pytest.mark.parametrize("python_version", [None, "3.10"])
async def test_init_venv(tmp_path, python_version):
    """
    GIVEN a path and a python version
    WHEN _init_venv_v2() is called
    THEN a python venv is initialised at path
    """
    venv_path = tmp_path / "fractal_test"
    venv_path.mkdir(exist_ok=True, parents=True)
    logger_name = "fractal"

    try:
        python_bin = await _init_venv_v2(
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
