from pathlib import Path
from typing import Optional

import pytest
from devtools import debug
from pydantic import BaseModel

from fractal_server.app.models.v2 import CollectionStateV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.tasks.v2.background_operations import (
    _check_task_files_exist,
)
from fractal_server.tasks.v2.background_operations import _download_package
from fractal_server.tasks.v2.background_operations import (
    background_collect_pip,
)
from fractal_server.tasks.v2.database_operations import _get_task_type
from fractal_server.tasks.v2.utils import _parse_wheel_filename


class _MockTaskCreateV2(BaseModel):
    name: str = "task_name"
    command_non_parallel: Optional[str] = None
    command_parallel: Optional[str] = None


def test_get_task_type():

    task = _MockTaskCreateV2(command_non_parallel="x")
    assert _get_task_type(task) == "non_parallel"
    task = _MockTaskCreateV2(command_parallel="x")
    assert _get_task_type(task) == "parallel"
    task = _MockTaskCreateV2(command_parallel="x", command_non_parallel="y")
    assert _get_task_type(task) == "compound"


def test_check_task_files_exist(tmp_path):
    existing_path = tmp_path / "existing.py"
    missing_path = tmp_path / "missing.py"
    existing_path.touch()
    existing_path = existing_path.as_posix()
    missing_path = missing_path.as_posix()
    # Success
    _check_task_files_exist(
        task_list=[
            _MockTaskCreateV2(command_non_parallel=f"py {existing_path}"),
            _MockTaskCreateV2(command_parallel=f"py {existing_path}"),
        ]
    )
    # Failures
    with pytest.raises(FileNotFoundError) as e:
        _check_task_files_exist(
            task_list=[
                _MockTaskCreateV2(command_non_parallel=f"py {missing_path}")
            ]
        )
    assert "missing file" in str(e.value)
    with pytest.raises(FileNotFoundError) as e:
        _check_task_files_exist(
            task_list=[
                _MockTaskCreateV2(command_parallel=f"py {missing_path}")
            ]
        )
    assert "missing file" in str(e.value)


def test_parse_wheel_filename():
    with pytest.raises(
        ValueError,
        match="Input must be a filename, not a full path",
    ):
        _parse_wheel_filename(wheel_filename="/tmp/something.whl")


async def test_download_package(tmp_path: Path, current_py_version: str):
    PACKAGE_VERSION = "1.0.1"
    PACKAGE_NAME = "fractal_tasks_core"
    wheel_path = await _download_package(
        python_version=current_py_version,
        pkg_name=PACKAGE_NAME,
        version=PACKAGE_VERSION,
        dest=(tmp_path / "wheel1"),
    )
    debug(wheel_path)
    assert wheel_path.exists()
    info = _parse_wheel_filename(wheel_filename=wheel_path.name)
    assert info == dict(distribution=PACKAGE_NAME, version=PACKAGE_VERSION)


async def test_background_collect_pip_existing_file(tmp_path, db, first_user):
    # Prepare db objects
    path = tmp_path / "something"
    task_group = TaskGroupV2(
        pkg_name="pkg",
        version="1.2.3",
        origin="pypi",
        path=path.as_posix(),
        user_id=first_user.id,
    )
    db.add(task_group)
    await db.commit()
    await db.refresh(task_group)
    db.expunge(task_group)
    state = CollectionStateV2(taskgroupv2_id=task_group.id)
    db.add(state)
    await db.commit()
    await db.refresh(state)
    db.expunge(state)
    # Create task_group.path
    path.mkdir()
    # Run background task
    await background_collect_pip(
        task_group=task_group,
        state_id=state.id,
    )
    # Verify that collection failed
    state = await db.get(CollectionStateV2, state.id)
    debug(state)
    assert state.data["status"] == "fail"
    assert "File exists" in state.data["log"]
    # Verify that foreign key was set to None
    assert state.taskgroupv2_id is None
