import json
import shlex
import subprocess
from pathlib import Path

import pytest
from sqlalchemy.orm import Session as DBSyncSession

from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.schemas.v2 import ManifestV2
from fractal_server.app.schemas.v2 import TaskCreateV2
from fractal_server.tasks.v2.background_operations import _insert_tasks
from fractal_server.tasks.v2.background_operations import (
    _prepare_tasks_metadata,
)


def run_cmd(cmd: str):
    res = subprocess.run(  # nosec
        shlex.split(cmd),
        capture_output=True,
        encoding="utf8",
    )
    if res.returncode != 0:
        raise subprocess.CalledProcessError(
            res.returncode, cmd=cmd, output=res.stdout, stderr=res.stderr
        )
    return res.stdout


@pytest.fixture(scope="session")
def fractal_tasks_mock_collection(tmpdir_factory, testdata_path) -> Path:

    base_dir = Path(tmpdir_factory.getbasetemp())
    venv_dir = base_dir / "venv"
    venv_python = venv_dir / "bin/python"

    if not venv_dir.exists():
        whl = (
            testdata_path.parent
            / "v2/fractal_tasks_mock/dist"
            / "fractal_tasks_mock-0.0.1-py3-none-any.whl"
        ).as_posix()
        run_cmd(f"python3.9 -m venv {venv_dir}")
        run_cmd(f"{venv_python} -m pip install {whl}")

    package_root = venv_dir / "lib/python3.9/site-packages/fractal_tasks_mock"

    with open(package_root / "__FRACTAL_MANIFEST__.json", "r") as f:
        manifest_dict = json.load(f)

    manifest = ManifestV2(**manifest_dict)
    task_list: list[TaskCreateV2] = _prepare_tasks_metadata(
        package_manifest=manifest,
        package_source="pytest",
        python_bin=venv_python,
        package_root=package_root,
    )

    return dict(python_bin=venv_python, task_list=task_list)


@pytest.fixture
def fractal_tasks_mock_db(
    fractal_tasks_mock_collection, db_sync: DBSyncSession
) -> dict[str, TaskV2]:

    task_list_db: list[TaskV2] = _insert_tasks(
        fractal_tasks_mock_collection["task_list"], db_sync
    )
    return {task.name: task for task in task_list_db}


@pytest.fixture(scope="function")
def relink_python_interpreter_v2(fractal_tasks_mock_db):
    """
    Rewire python executable in tasks
    """
    import os
    from pathlib import Path

    import logging
    from .fixtures_slurm import HAS_LOCAL_SBATCH

    if not HAS_LOCAL_SBATCH:

        logger = logging.getLogger("RELINK")
        logger.setLevel(logging.INFO)
        first_task = next(iter(fractal_tasks_mock_db.values()))
        task_python = Path(first_task.command_non_parallel.split()[0])
        logger.warning(f"Original tasks Python: {task_python.as_posix()}")

        actual_task_python = os.readlink(task_python)
        logger.warning(
            f"Actual tasks Python (after readlink): {actual_task_python}"
        )

        # NOTE that the docker container in the CI only has python3.9
        # installed, therefore we explicitly hardcode this version here, to
        # make debugging easier
        # NOTE that the slurm-node container also installs a version of
        # fractal-tasks-core
        task_python.unlink()
        new_actual_task_python = "/usr/bin/python3.9"
        task_python.symlink_to(new_actual_task_python)
        logger.warning(f"New tasks Python: {new_actual_task_python}")

        yield

        task_python.unlink()
        task_python.symlink_to(actual_task_python)
        logger.warning(
            f"Restored link from "
            f"{task_python.as_posix()} to {os.readlink(task_python)}"
        )
    else:
        yield
