import logging
from typing import Any

import pytest
from v2_mock_models import TaskV2Mock

from fractal_server.app.runner.v2._local import LocalRunner
from fractal_server.app.schemas.v2 import TaskCreateV2
from fractal_server.tasks.v2.utils_database import _get_task_type


@pytest.fixture()
def local_runner():
    with LocalRunner() as r:
        yield r


def _run_cmd(*, cmd: str, label: str) -> str:
    import subprocess  # nosec
    import shlex

    res = subprocess.run(
        shlex.split(cmd),
        capture_output=True,
        encoding="utf8",
    )
    if not res.returncode == 0:
        logging.error(f"[{label}] FAIL")
        logging.error(f"[{label}] command: {cmd}")
        logging.error(f"[{label}] stdout: {res.stdout}")
        logging.error(f"[{label}] stderr: {res.stderr}")
        raise ValueError(res)
    return res.stdout


@pytest.fixture
def fractal_tasks_mock_no_db(
    fractal_tasks_mock_collection: dict[str, Any],
) -> dict[str, TaskV2Mock]:
    """
    We use this fixture in tests that operate on Mock models,
    and therefore do not need the tasks to be in the database.
    """
    task_list: list[TaskCreateV2] = fractal_tasks_mock_collection["task_list"]
    return {
        task.name: TaskV2Mock(
            id=_id,
            type=_get_task_type(task),
            **task.model_dump(),
        )
        for _id, task in enumerate(task_list)
    }
