import json
import logging
import os
import shlex
import subprocess
from pathlib import Path
from typing import Any

import pytest
from pytest import TempdirFactory
from sqlalchemy.orm import Session as DBSyncSession
from sqlmodel import select

from fractal_server.app.models import UserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import Profile
from fractal_server.app.models.v2 import Resource
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.schemas.v2 import ManifestV2
from fractal_server.app.schemas.v2 import TaskCreate
from fractal_server.app.schemas.v2 import TaskGroupCreate
from fractal_server.tasks.v2.utils_background import prepare_tasks_metadata
from fractal_server.tasks.v2.utils_database import (
    create_db_tasks_and_update_task_group_sync,
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
def fractal_tasks_mock_collection(
    tmpdir_factory: TempdirFactory,
    testdata_path: Path,
    current_py_version: str,
) -> dict[str, Any]:
    """
    Session scoped fixture that builds a Python venv and use it to collect the
    Fractal Tasks of the 'fractal_tasks_mock-0.0.1-py3-none-any.whl' package.
    """
    base_dir = Path(tmpdir_factory.getbasetemp())
    venv_dir = base_dir / "task_venv"
    venv_python = venv_dir / "bin/python"

    if not venv_dir.exists():
        whl = (
            testdata_path.parent
            / "v2/fractal_tasks_mock/dist"
            / "fractal_tasks_mock-0.0.1-py3-none-any.whl"
        ).as_posix()
        run_cmd(f"python{current_py_version} -m venv {venv_dir}")
        run_cmd(f"{venv_python} -m pip install {whl}")

    package_root = (
        venv_dir
        / f"lib/python{current_py_version}/site-packages/fractal_tasks_mock"
    )

    with open(package_root / "__FRACTAL_MANIFEST__.json") as f:
        manifest_dict = json.load(f)

    manifest = ManifestV2(**manifest_dict)
    task_list: list[TaskCreate] = prepare_tasks_metadata(
        package_manifest=manifest,
        python_bin=venv_python,
        package_root=package_root,
    )

    return dict(
        python_bin=venv_python,
        package_root=package_root,
        manifest=manifest,
        task_list=task_list,
    )


@pytest.fixture(scope="function")
def fractal_tasks_mock_db(
    fractal_tasks_mock_collection,
    db_sync: DBSyncSession,
    first_user: UserOAuth,
    default_user_group: UserGroup,
) -> dict[str, TaskV2]:
    res = db_sync.execute(
        select(Resource.id)
        .join(Profile, Profile.resource_id == Resource.id)
        .where(Profile.id == first_user.profile_id)
    )
    resource_id = res.scalar_one()

    task_group_obj = TaskGroupCreate(
        origin="other",
        pkg_name="fractal_tasks_mock",
        user_id=first_user.id,
        resource_id=resource_id,
        user_group_id=default_user_group.id,
    )
    task_group = TaskGroupV2(**task_group_obj.model_dump())
    db_sync.add(task_group)
    db_sync.commit()
    db_sync.refresh(task_group)
    db_sync.expunge(task_group)

    task_group = create_db_tasks_and_update_task_group_sync(
        task_group_id=task_group.id,
        task_list=fractal_tasks_mock_collection["task_list"],
        db=db_sync,
    )
    return {task.name: task for task in task_group.task_list}


@pytest.fixture(scope="function")
def relink_python_interpreter(
    fractal_tasks_mock_collection, current_py_version: str
):
    """
    Rewire python executable in tasks
    """

    logger = logging.getLogger("RELINK")
    logger.setLevel(logging.INFO)
    task_python = fractal_tasks_mock_collection["python_bin"]
    logger.warning(f"Original tasks Python: {task_python.as_posix()}")
    actual_task_python = os.readlink(task_python)
    logger.warning(
        f"Actual tasks Python (after readlink): {actual_task_python}"
    )
    task_python.unlink()
    new_actual_task_python = (
        f"/.venv{current_py_version}/bin/python{current_py_version}"
    )
    task_python.symlink_to(new_actual_task_python)
    logger.warning(f"New tasks Python: {new_actual_task_python}")
    yield
    task_python.unlink()
    task_python.symlink_to(actual_task_python)
    logger.warning(
        f"Restored link from "
        f"{task_python.as_posix()} to {os.readlink(task_python)}"
    )
