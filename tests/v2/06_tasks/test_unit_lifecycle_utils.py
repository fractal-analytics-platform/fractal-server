import pytest
from pydantic import BaseModel

from fractal_server.tasks.v2.local._utils import (
    check_task_files_exist,
)
from fractal_server.tasks.v2.utils_database import _get_task_type


class _MockTaskCreateV2(BaseModel):
    name: str = "task_name"
    command_non_parallel: str | None = None
    command_parallel: str | None = None


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
