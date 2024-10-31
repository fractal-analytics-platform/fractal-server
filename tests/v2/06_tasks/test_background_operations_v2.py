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


async def test_collect_pip_existing_file(tmp_path, db, first_user):
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


async def test_collect_pip_local_fail_rmtree(
    tmp_path, db, first_user, testdata_path, current_py_version
):
    from unittest.mock import patch

    with patch("shutil.rmtree", side_effect=Exception("Broken rm")):
        # Prepare db objects
        path = tmp_path / "rmtree-error"
        task_group = TaskGroupV2(
            pkg_name="fractal-tasks-mock",
            version="0.0.1",
            origin="local",
            wheel_path=(
                testdata_path.parent
                / (
                    "v2/fractal_tasks_fail/invalid_manifest/dist/"
                    "fractal_tasks_mock-0.0.1-py3-none-any.whl"
                )
            ).as_posix(),
            python_version=current_py_version,
            path=path.as_posix(),
            venv_path=(path / "venv").as_posix(),
            user_id=first_user.id,
        )
        debug(task_group)
        db.add(task_group)
        await db.commit()
        await db.refresh(task_group)
        db.expunge(task_group)
        state = CollectionStateV2(taskgroupv2_id=task_group.id)
        db.add(state)
        await db.commit()
        await db.refresh(state)
        db.expunge(state)
        # Run background task
        await collect_package_local(
            task_group=task_group,
            state_id=state.id,
        )
        # Verify that collection failed
        state = await db.get(CollectionStateV2, state.id)
        assert state.data["status"] == "fail"
        assert "Broken rm" in state.data["log"]
        assert path.exists()
