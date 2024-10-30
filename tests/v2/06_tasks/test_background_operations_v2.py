from typing import Optional

import pytest
from devtools import debug
from pydantic import BaseModel

from fractal_server.app.models.v2 import CollectionStateV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.tasks.v2.collection_local import collect_package_local
from fractal_server.tasks.v2.database_operations import _get_task_type
from fractal_server.tasks.v2.utils_background import (
    check_task_files_exist,
)
from fractal_server.tasks.v2.utils_package_names import _parse_wheel_filename


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
    check_task_files_exist(
        task_list=[
            _MockTaskCreateV2(command_non_parallel=f"py {existing_path}"),
            _MockTaskCreateV2(command_parallel=f"py {existing_path}"),
        ]
    )
    # Failures
    with pytest.raises(FileNotFoundError) as e:
        check_task_files_exist(
            task_list=[
                _MockTaskCreateV2(command_non_parallel=f"py {missing_path}")
            ]
        )
    assert "missing file" in str(e.value)
    with pytest.raises(FileNotFoundError) as e:
        check_task_files_exist(
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


async def test_background_collect_pip_existing_file(tmp_path, db, first_user):
    # Prepare db objects
    path = tmp_path / "something"
    task_group = TaskGroupV2(
        pkg_name="pkg",
        version="1.2.3",
        origin="pypi",
        path=path.as_posix(),
        venv_path=(path / "venv").as_posix(),
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
    await collect_package_local(
        task_group=task_group,
        state_id=state.id,
    )
    # Verify that collection failed
    state = await db.get(CollectionStateV2, state.id)
    debug(state)
    assert state.data["status"] == "fail"
    # Verify that foreign key was set to None
    assert state.taskgroupv2_id is None
