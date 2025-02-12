import logging

import pytest
from v2_mock_models import TaskV2Mock

from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.runner.v2._local import FractalThreadPoolExecutor


@pytest.fixture()
def executor():
    with FractalThreadPoolExecutor() as e:
        yield e


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
    fractal_tasks_mock_collection: dict[str, TaskV2],
) -> dict[str, TaskV2Mock]:
    """
    We use this fixture in tests that operate on Mock models,
    and therefore do not need the tasks to be in the database.
    """
    return {
        task.name: TaskV2Mock(id=_id, **task.model_dump())
        for _id, task in enumerate(fractal_tasks_mock_collection["task_list"])
    }
