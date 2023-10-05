import asyncio
import json
from pathlib import Path
from typing import AsyncGenerator
from typing import Optional

import pytest
from devtools import debug  # noqa
from pydantic import BaseModel
from pydantic import validator

from .fixtures_server import HAS_LOCAL_SBATCH


class MockTask(BaseModel):
    name: str
    command: str
    parallelization_level: Optional[str] = None
    meta: Optional[dict] = {}


class MockWorkflowTask(BaseModel):
    order: int = 0
    task: MockTask
    args: dict = {}
    meta: dict = {}
    executor: Optional[str] = "default"

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
        return bool(self.task.parallelization_level)

    @property
    def parallelization_level(self) -> Optional[str]:
        return self.task.parallelization_level


async def execute_command(cmd, **kwargs):
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        **kwargs,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(stderr.decode("UTF-8"))
    return stdout.decode("UTF-8").strip()


@pytest.fixture(scope="session")
async def dummy_task_package(
    testdata_path,
    tmp_path_factory,
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
    testdata_path, tmp_path, override_settings
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
    manifest["manifest_version"] = 2
    with MANIFEST_PATH.open("w") as f:
        json.dump(manifest, f)

    await execute_command("poetry build", cwd=PACKAGE_PATH)
    wheel_relative = await execute_command("ls dist/*.whl", cwd=PACKAGE_PATH)
    wheel_path = PACKAGE_PATH / wheel_relative
    yield wheel_path


@pytest.fixture
async def dummy_task_package_missing_manifest(
    testdata_path, tmp_path, override_settings
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


@pytest.fixture(scope="function")
async def install_dummy_packages(
    tmp777_session_path, dummy_task_package, override_settings
):
    """
    NOTE that the system python3 on the slurm containers (AKA /usr/bin/python3)
    is 3.9, and relink_python_interpreter will map to it. Therefore this
    fixture must always install dummy_task_package with this version.

    FIXME: for how this is written, it requires that python3.9 and its venv
    module are available on the machine that is running pytest (and then
    fractal-server), see
    https://github.com/fractal-analytics-platform/fractal-server/issues/498
    """

    from fractal_server.tasks.collection import (
        inspect_package,
        create_package_dir_pip,
        create_package_environment_pip,
    )
    from fractal_server.tasks.collection import _TaskCollectPip

    task_pkg = _TaskCollectPip(
        package=dummy_task_package.as_posix(),
        python_version="3.9",
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
async def collect_packages(db_sync, install_dummy_packages, override_settings):
    from fractal_server.app.api.v1.task_collection import _insert_tasks

    tasks = await _insert_tasks(task_list=install_dummy_packages, db=db_sync)
    return tasks


@pytest.fixture(scope="function")
def relink_python_interpreter(collect_packages, override_settings):
    """
    Rewire python executable in tasks

    """
    import os
    import logging

    if not HAS_LOCAL_SBATCH:

        logger = logging.getLogger("RELINK")
        logger.setLevel(logging.INFO)

        task = collect_packages[0]
        task_python = Path(task.command.split()[0])
        orig_python = os.readlink(task_python)
        logger.warning(
            f"RELINK: Original status: {task_python=} -> {orig_python}"
        )
        task_python.unlink()
        # NOTE that the docker container in the CI only has python3.9
        # installed, therefore we explicitly hardcode this version here, to
        # make debugging easier
        task_python.symlink_to("/usr/bin/python3.9")
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
    else:
        yield
