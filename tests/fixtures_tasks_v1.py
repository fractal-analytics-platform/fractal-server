import json
from pathlib import Path
from typing import AsyncGenerator

import pytest
from devtools import debug  # noqa
from pydantic import BaseModel
from pydantic import validator

from fractal_server.tasks.v1.endpoint_operations import create_package_dir_pip
from fractal_server.tasks.v1.endpoint_operations import inspect_package
from tests.execute_command import execute_command


class MockTask(BaseModel):
    name: str
    command: str
    meta: dict | None = {}

    def model_dump(self, *args, **kwargs):
        return self.dict(*args, **kwargs)

    @property
    def parallelization_level(self) -> str | None:
        try:
            return self.meta["parallelization_level"]
        except KeyError:
            return None

    @property
    def is_parallel(self) -> bool:
        return bool(self.parallelization_level)


class MockWorkflowTask(BaseModel):
    id: int = 99
    order: int = 0
    task: MockTask
    args: dict = {}
    meta: dict = {}
    executor: str | None = "default"

    def model_dump(self, *args, **kwargs):
        return self.dict(*args, **kwargs)

    @validator("meta", pre=True)
    def merge_meta(cls, meta, values):
        """
        This validator merges the task.meta and meta dictionaries, in the same
        way as it takes place in Workflow.insert_task.
        """
        task_meta = values.get("task").meta
        if task_meta:
            meta = {**task_meta, **meta}
        return meta

    @property
    def is_parallel(self) -> bool:
        return self.task.is_parallel

    @property
    def parallelization_level(self) -> str | None:
        return self.task.parallelization_level


@pytest.fixture(scope="session")
async def dummy_task_package(
    testdata_path, tmp_path_factory
) -> AsyncGenerator[Path, None]:
    """
    Yields
    ------
    wheel_path : Path
        the path to the built wheel package
    """
    from .data import tasks_dummy as task_package

    PACKAGE_TEMPLATE_PATH = testdata_path / "fractal-tasks-dummy"
    PACKAGE_PATH = tmp_path_factory.mktemp("fractal-tasks-dummy")
    SOURCE_PATH = PACKAGE_PATH / "fractal_tasks_dummy"
    DUMMY_PACKAGE = Path(task_package.__file__).parent

    # copy template to temp
    await execute_command(f"cp -r {PACKAGE_TEMPLATE_PATH}/* {PACKAGE_PATH}")
    # copy content of task_package to PACKAGE_PATH
    await execute_command(f"cp {DUMMY_PACKAGE}/*.* {SOURCE_PATH}")
    await execute_command("poetry build", cwd=SOURCE_PATH)
    wheel_relative = await execute_command("ls dist/*.whl", cwd=PACKAGE_PATH)
    wheel_path = PACKAGE_PATH / wheel_relative
    yield wheel_path


@pytest.fixture
async def dummy_task_package_invalid_manifest(
    testdata_path, tmp_path
) -> AsyncGenerator[Path, None]:
    from .data import tasks_dummy as task_package

    PACKAGE_TEMPLATE_PATH = testdata_path / "fractal-tasks-dummy"
    PACKAGE_PATH = tmp_path / "invalid_manifest/fractal-tasks-dummy"
    PACKAGE_PATH.parent.mkdir(parents=True)
    SOURCE_PATH = PACKAGE_PATH / "fractal_tasks_dummy"
    DUMMY_PACKAGE = Path(task_package.__file__).parent

    # copy template to temp
    await execute_command(f"cp -r {PACKAGE_TEMPLATE_PATH} {PACKAGE_PATH}")
    # copy content of task_package to PACKAGE_PATH
    await execute_command(f"cp {DUMMY_PACKAGE}/*.* {SOURCE_PATH}")

    # Make manifest invalid (change version from 1 to 2)
    MANIFEST_PATH = SOURCE_PATH / "__FRACTAL_MANIFEST__.json"
    with MANIFEST_PATH.open("r") as f:
        manifest = json.load(f)
    manifest["manifest_version"] = 99999
    with MANIFEST_PATH.open("w") as f:
        json.dump(manifest, f)

    await execute_command("poetry build", cwd=PACKAGE_PATH)
    wheel_relative = await execute_command("ls dist/*.whl", cwd=PACKAGE_PATH)
    wheel_path = PACKAGE_PATH / wheel_relative
    yield wheel_path


@pytest.fixture
async def dummy_task_package_missing_manifest(
    testdata_path, tmp_path
) -> AsyncGenerator[Path, None]:
    from .data import tasks_dummy as task_package

    PACKAGE_TEMPLATE_PATH = testdata_path / "fractal-tasks-dummy"
    PACKAGE_PATH = tmp_path / "missing_manifest/fractal-tasks-dummy"
    PACKAGE_PATH.parent.mkdir(parents=True)
    SOURCE_PATH = PACKAGE_PATH / "fractal_tasks_dummy"
    DUMMY_PACKAGE = Path(task_package.__file__).parent

    # copy template to temp
    await execute_command(f"cp -r {PACKAGE_TEMPLATE_PATH} {PACKAGE_PATH}")
    # copy content of task_package to PACKAGE_PATH
    await execute_command(f"cp {DUMMY_PACKAGE}/*.* {SOURCE_PATH}")

    # Remove manifest
    MANIFEST_PATH = SOURCE_PATH / "__FRACTAL_MANIFEST__.json"
    MANIFEST_PATH.unlink()

    await execute_command("poetry build", cwd=PACKAGE_PATH)
    wheel_relative = await execute_command("ls dist/*.whl", cwd=PACKAGE_PATH)
    wheel_path = PACKAGE_PATH / wheel_relative
    yield wheel_path


@pytest.fixture(scope="session")
async def install_dummy_packages(
    tmp777_session_path, dummy_task_package, current_py_version: str
):

    from fractal_server.tasks.v1.background_operations import (
        create_package_environment_pip,
    )
    from fractal_server.tasks.v1._TaskCollectPip import _TaskCollectPip

    task_pkg = _TaskCollectPip(
        package=dummy_task_package.as_posix(),
        python_version=current_py_version,
    )

    pkg_info = inspect_package(dummy_task_package)
    task_pkg.package_version = pkg_info["pkg_version"]
    task_pkg.package_name = pkg_info["pkg_name"]
    task_pkg.package_manifest = pkg_info["pkg_manifest"]
    task_pkg.check()

    venv_path = create_package_dir_pip(task_pkg=task_pkg)
    task_list = await create_package_environment_pip(
        venv_path=venv_path,
        task_pkg=task_pkg,
        logger_name="dummy",
    )

    return task_list


@pytest.fixture(scope="function")
async def collect_packages(db_sync, install_dummy_packages):
    from fractal_server.tasks.v1.background_operations import _insert_tasks

    tasks = await _insert_tasks(task_list=install_dummy_packages, db=db_sync)
    return tasks


@pytest.fixture(scope="function")
def relink_python_interpreter_v1(collect_packages, current_py_version: str):
    """
    Rewire python executable in tasks

    """
    import os
    import logging

    logger = logging.getLogger("RELINK")
    logger.setLevel(logging.INFO)
    task = collect_packages[0]
    task_python = Path(task.command.split()[0])
    orig_python = os.readlink(task_python)
    logger.warning(f"RELINK: Original status: {task_python=} -> {orig_python}")
    task_python.unlink()
    task_python.symlink_to(
        f"/.venv{current_py_version}/bin/python{current_py_version}"
    )
    logger.warning(
        f"RELINK: Updated status: {task_python=} -> "
        f"{os.readlink(task_python.as_posix())}"
    )
    yield
    task_python.unlink()
    task_python.symlink_to(orig_python)
    logger.warning(
        f"RELINK: Restore original: {task_python=} -> "
        f"{os.readlink(task_python.as_posix())}"
    )
