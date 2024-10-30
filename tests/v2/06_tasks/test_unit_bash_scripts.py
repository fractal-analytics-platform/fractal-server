from pathlib import Path

import pytest
from devtools import debug

from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.config import get_settings
from fractal_server.logger import set_logger
from fractal_server.syringe import Inject
from fractal_server.tasks.utils import COLLECTION_FREEZE_FILENAME
from fractal_server.tasks.v2.utils_templates import customize_template
from fractal_server.utils import execute_command_sync
from tests.execute_command import execute_command


def test_template_1(tmp_path):

    replacements = {}
    script_path = tmp_path / "x.sh"
    customize_template(
        template_name="_1_create_venv.sh",
        replacements=replacements,
        script_path=script_path,
    )
    stdout = execute_command_sync(f"bash {script_path}")
    assert somefolder.exists()

    replacements = {}
    script_path = tmp_path / "x.sh"
    customize_template(
        template_name="_1_create_venv.sh",
        replacements=replacements,
        script_path=script_path,
    )
    with pytest.raises():
        execute_command_sync(f"bash {script_path}")


def test_template_3():
    pass

    # Re-use part of the cases listed in
    # test_pip_install_with_pinned_dependencies below


# async def test_pip_install_with_pinned_dependencies(tmp_path, caplog):
#     settings = Inject(get_settings)
#     PYTHON_VERSION = settings.FRACTAL_TASKS_PYTHON_DEFAULT_VERSION
#     pkg_path = tmp_path / "devtools/0.8.0/"
#     EXTRA = "pygments"

#     LOGGER_NAME = "pinned_dependencies"
#     logger = set_logger(LOGGER_NAME)
#     logger.propagate = True

#     # Create TaskGroupCreateV2 (to mimic TaskGroupV2 object)
#     common_attrs = dict(
#         pkg_name="devtools",
#         version="0.8.0",
#         wheel_path=None,
#         python_version=PYTHON_VERSION,
#         user_id=0,
#         origin="pypi",
#         path=pkg_path.as_posix(),
#         venv_path=(pkg_path / "venv").as_posix(),
#         pip_extras=EXTRA,
#     )
#     task_group = TaskGroupV2(**common_attrs)

#     # Prepare venv
#     Path(task_group.venv_path).mkdir(exist_ok=True, parents=True)
#     await _init_venv_v2(
#         venv_path=Path(task_group.venv_path),
#         python_version=task_group.python_version,
#         logger_name=LOGGER_NAME,
#     )

#     async def _aux(_task_group) -> str:
#         """pip install with pin and return version for EXTRA package"""
#         await _pip_install(
#             task_group=_task_group,
#             logger_name=LOGGER_NAME,
#         )

#         # Check freeze file
#         freeze_file = Path(_task_group.path) / COLLECTION_FREEZE_FILENAME
#         assert freeze_file.exists()
#         with freeze_file.open("r") as f:
#             freeze_data = f.read().splitlines()
#         debug(freeze_data)
#         extra_version = next(
#             line.split("==")[1]
#             for line in freeze_data
#             if line.lower().startswith(EXTRA.lower())
#         )
#         debug(extra_version)
#         # Clean up
#         python_bin = (Path(task_group.venv_path) / "bin/python").as_posix()
#         await execute_command(
#             f"{python_bin} -m pip uninstall {task_group.pkg_name} {EXTRA} -y"
#         )
#         return extra_version

#     # Case 0:
#     #   get default version of EXTRA, and then use it as a pin
#     DEFAULT_VERSION = await _aux(TaskGroupV2(**common_attrs))
#     caplog.clear()
#     pin = {EXTRA: DEFAULT_VERSION}
#     new_version = await _aux(
#         TaskGroupV2(**common_attrs, pinned_package_versions=pin)
#     )
#     assert new_version == DEFAULT_VERSION
#     assert "Specific version required" in caplog.text
#     assert "already matches the pinned version" in caplog.text
#     caplog.clear()

#     # Case 1: pin EXTRA to a specific (non-default) version
#     PIN_VERSION = "2.0"
#     assert PIN_VERSION != DEFAULT_VERSION
#     pin = {EXTRA: PIN_VERSION}
#     new_version = await _aux(
#         TaskGroupV2(**common_attrs, pinned_package_versions=pin)
#     )

#     assert new_version == PIN_VERSION
#     assert "differs from pinned version" in caplog.text
#     assert f"pip install {EXTRA}" in caplog.text
#     caplog.clear()

#     # Case 2: bad pin with invalid EXTRA version
#     INVALID_EXTRA_VERSION = "123456789"
#     pin = {EXTRA: INVALID_EXTRA_VERSION}
#     with pytest.raises(RuntimeError) as error_info:
#         await _aux(TaskGroupV2(**common_attrs, pinned_package_versions=pin))
#     assert f"pip install {EXTRA}=={INVALID_EXTRA_VERSION}" in caplog.text
#     assert (
#         "Could not find a version that satisfies the requirement "
#         f"{EXTRA}=={INVALID_EXTRA_VERSION}"
#     ) in str(error_info.value)
#     caplog.clear()

#     # Case 3: bad pin with package which was not already installed
#     pin = {"pydantic": "1.0.0"}
#     with pytest.raises(RuntimeError) as error_info:
#         await _aux(TaskGroupV2(**common_attrs, pinned_package_versions=pin))
#     assert "pip show pydantic" in caplog.text
#     assert "Package(s) not found: pydantic" in str(error_info.value)
#     caplog.clear()
