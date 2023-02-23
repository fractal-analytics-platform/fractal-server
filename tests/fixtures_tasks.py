import asyncio
import sys
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Optional

import pytest
from pydantic import BaseModel

from .fixtures_server import check_python_has_venv
from .fixtures_server import HAS_LOCAL_SBATCH


class MockTask(BaseModel):
    name: str
    command: str
    parallelization_level: Optional[str] = None


class MockWorkflowTask(BaseModel):
    order: int = 0
    task: MockTask
    arguments: Dict = {}
    executor: Optional[str] = "default"

    @property
    def is_parallel(self) -> bool:
        return bool(self.task.parallelization_level)

    @property
    def parallelization_level(self) -> Optional[str]:
        return self.task.parallelization_level

    def assemble_args(self, extra: Dict[str, Any] = None):
        """
        Merge of `extra` arguments and `self.arguments`.

        Return
        ------
        full_arsgs (Dict):
            A dictionary consisting of the merge of `extra` and
            self.arguments.
        """
        full_args = {}
        if extra:
            full_args.update(extra)
        full_args.update(self.arguments)
        return full_args


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
async def dummy_task_package(testdata_path, tmp_path_factory) -> Path:
    """
    Yields
    ------
    wheel_path : Path
        the path to the built wheel package
    """
    from fractal_server import tasks as task_package

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
async def dummy_task_package_invalid_manifest(testdata_path, tmp_path) -> Path:
    """
    Yields
    ------
    wheel_path : Path
        the path to the built wheel package
    """
    from fractal_server import tasks as task_package

    PACKAGE_TEMPLATE_PATH = testdata_path / "fractal-tasks-dummy"
    PACKAGE_PATH = tmp_path / "fractal-tasks-dummy"
    SOURCE_PATH = PACKAGE_PATH / "fractal_tasks_dummy"
    DUMMY_PACKAGE = Path(task_package.__file__).parent

    # copy template to temp
    await execute_command(f"cp -r {PACKAGE_TEMPLATE_PATH} {PACKAGE_PATH}")
    # copy content of task_package to PACKAGE_PATH
    await execute_command(f"cp {DUMMY_PACKAGE}/*.* {SOURCE_PATH}")

    # Make manifest invalid
    MANIFEST_PATH = SOURCE_PATH / "__FRACTAL_MANIFEST__.json"
    with MANIFEST_PATH.open("w") as f:
        f.write("invalid manifest")

    await execute_command("poetry build", cwd=PACKAGE_PATH)
    wheel_relative = await execute_command("ls dist/*.whl", cwd=PACKAGE_PATH)
    wheel_path = PACKAGE_PATH / wheel_relative
    yield wheel_path


@pytest.fixture(scope="session")
async def install_dummy_packages(tmp777_session_path, dummy_task_package):
    """
    NOTE that the system python3 on the slurm containers (AKA /usr/bin/python3)
    is 3.9, and relink_python_interpreter will map to it. Therefore this
    fixture must always install dummy_task_package with this version.

    Also note that the check_python_has_venv function will verify that
    sys.executable (to be used for the tasks environment) has the venv module.
    """

    from fractal_server.tasks.collection import (
        _create_venv_install_package,
        load_manifest,
    )
    from fractal_server.tasks.collection import _TaskCollectPip

    python_test_path = tmp777_session_path("check_python_has_venv")
    python_test_path.mkdir(exist_ok=True, parents=True)
    check_python_has_venv(sys.executable, python_test_path)

    venv_path = tmp777_session_path("dummy")
    venv_path.mkdir(exist_ok=True, parents=True)
    task_pkg = _TaskCollectPip(
        package=dummy_task_package.as_posix(),
        python_version="3.9",
    )

    python_bin, package_root = await _create_venv_install_package(
        path=venv_path,
        task_pkg=task_pkg,
        logger_name="test",
    )

    task_list = load_manifest(
        package_root=package_root,
        python_bin=python_bin,
        source="test_source",
    )
    return task_list


@pytest.fixture(scope="function")
async def collect_packages(db, install_dummy_packages):
    from fractal_server.app.api.v1.task import _insert_tasks

    tasks = await _insert_tasks(task_list=install_dummy_packages, db=db)
    return tasks


@pytest.fixture(scope="function")
def relink_python_interpreter(collect_packages):
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
