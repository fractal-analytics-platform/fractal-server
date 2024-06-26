import logging
import shutil
from pathlib import Path
from typing import Optional

import pytest
from devtools import debug  # noqa

from fractal_server.app.models.v2 import TaskV2
from fractal_server.config import get_settings
from fractal_server.syringe import Inject
from fractal_server.tasks.utils import get_collection_path
from fractal_server.tasks.utils import get_log_path
from fractal_server.tasks.v2._TaskCollectPip import _TaskCollectPip
from tests.execute_command import execute_command


PREFIX = "api/v2/task"


@pytest.mark.parametrize(
    "python_version",
    [
        None,
        pytest.param(
            "3.10",
            marks=pytest.mark.skipif(
                not shutil.which("python3.10"), reason="No python3.10 on host"
            ),
        ),
    ],
)
async def test_task_collection(
    db,
    client,
    MockCurrentUser,
    override_settings_factory,
    tmp_path: Path,
    testdata_path: Path,
    python_version: Optional[str],
):
    override_settings_factory(
        # Use scoped path, so that repeated test executions (due to
        # parametrization) always start from a clean state
        FRACTAL_TASKS_DIR=(tmp_path / "FRACTAL_TASKS_DIR"),
        # Set logging to CRITICAL, and then make sure that task-collection
        # logs were included
        FRACTAL_LOGGING_LEVEL=logging.CRITICAL,
    )

    # Prepare absolute path to wheel file
    wheel_path = (
        testdata_path.parent
        / "v2/fractal_tasks_mock/dist"
        / "fractal_tasks_mock-0.0.1-py3-none-any.whl"
    )

    # Prepare and validate payload
    payload = dict(package=wheel_path.as_posix(), package_extras="my_extra")

    # Set python_version, if missing
    if python_version is None:
        settings = Inject(get_settings)
        python_version = settings.FRACTAL_TASKS_PYTHON_DEFAULT_VERSION
    payload["python_version"] = python_version

    # Prepare expected source
    EXPECTED_SOURCE = (
        f"pip_local:fractal_tasks_mock:0.0.1:my_extra:py{python_version}"
    )
    debug(EXPECTED_SOURCE)

    # Validate payload
    debug(payload)
    _TaskCollectPip(**payload)

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        # Trigger task collection
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            json=payload,
        )
        debug(res.json())
        assert res.status_code == 201
        assert res.json()["data"]["status"] == "pending"
        state = res.json()
        state_id = state["id"]
        data = state["data"]
        venv_path = Path(data["venv_path"])
        debug(venv_path)
        assert "fractal-tasks-mock" in data["venv_path"]

        # Get collection info
        res = await client.get(f"{PREFIX}/collect/{state_id}/")
        assert res.status_code == 200
        state = res.json()
        data = state["data"]
        task_list = data["task_list"]
        for i, task in enumerate(task_list):
            if i == 0:
                assert task["meta_non_parallel"] == {"key1": "value1"}
                assert task["meta_parallel"] == {"key2": "value2"}
            else:
                assert task["meta_non_parallel"] == task["meta_parallel"] == {}
        task_names = (t["name"] for t in task_list)
        debug(task_names)
        if data["status"] != "OK":
            print(data["log"])
        assert data["status"] == "OK"
        # Check that log were written, even with CRITICAL logging level
        log = data["log"]
        assert log is not None
        # Check that my_extra was included, in a local-package collection
        assert ".whl[my_extra]" in log

        # Check on-disk files
        settings = Inject(get_settings)
        full_path = settings.FRACTAL_TASKS_DIR / venv_path
        assert get_collection_path(full_path).exists()
        assert get_log_path(full_path).exists()

        # Check Python version
        if python_version:
            python_bin = task_list[0]["command_non_parallel"].split()[0]
            version = await execute_command(f"{python_bin} --version")
            assert python_version in version

        # Check task source
        for task in task_list:
            print(task["source"])
            assert task["source"].startswith(EXPECTED_SOURCE)

        # Check task type
        for task in task_list:
            if task["command_non_parallel"] is None:
                expected_type = "parallel"
            elif task["command_parallel"] is None:
                expected_type = "non_parallel"
            else:
                expected_type = "compound"
            assert task["type"] == expected_type

        # Check that argument JSON schemas are present
        for task in task_list:
            if task["command_non_parallel"] is not None:
                assert task["args_schema_non_parallel"] is not None
            if task["command_parallel"] is not None:
                assert task["args_schema_parallel"] is not None

        # Collect again (already installed)
        res = await client.post(f"{PREFIX}/collect/pip/", json=payload)
        debug(res.json())
        assert res.status_code == 200
        state = res.json()
        data = state["data"]
        assert data["info"] == "Already installed"

        # Check that *verbose* collection info contains logs
        res = await client.get(f"{PREFIX}/collect/{state_id}/?verbose=true")
        assert res.status_code == 200
        assert res.json()["data"]["log"] is not None

        # Modify a task source (via DB, since endpoint cannot modify source)
        db_task = await db.get(TaskV2, task_list[0]["id"])
        db_task.source = "EDITED_SOURCE"
        await db.commit()
        await db.close()

        # Collect again, and check that collection fails
        res = await client.post(f"{PREFIX}/collect/pip/", json=payload)
        debug(res.json())
        assert res.status_code == 422
