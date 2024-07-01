import shutil

import pytest
from devtools import debug

PREFIX = "/api/v2"


@pytest.fixture
async def fractal_tasks_mock(
    db_sync,
    client,
    MockCurrentUser,
    testdata_path,
    override_settings_factory,
    tmp_path_factory,
) -> dict:
    """
    Set a session-scoped FRACTAL_TASKS_DIR folder, and then:
    1. If the fractal-tasks-mock folder exists, re-use existing venv and
       manually add tasks to the DB.
    2. Else, perform task collection through the API.
    """

    import logging
    import json
    from fractal_server.app.schemas.v2.task_collection import (
        TaskCollectStatusV2,
    )
    from fractal_server.app.schemas.v2.task import TaskCreateV2
    from fractal_server.tasks.v2.background_operations import _insert_tasks
    from fractal_server.tasks.utils import COLLECTION_FILENAME

    # Create a session-scoped FRACTAL_TASKS_DIR folder
    basetemp = tmp_path_factory.getbasetemp()
    FRACTAL_TASKS_DIR = basetemp / "FRACTAL_TASKS_DIR"
    FRACTAL_TASKS_DIR.mkdir(exist_ok=True)
    override_settings_factory(
        FRACTAL_TASKS_DIR=FRACTAL_TASKS_DIR,
        FRACTAL_TASKS_PYTHON_3_9=shutil.which("python3.9"),
    )
    FRACTAL_TASKS_MOCK_DIR = (
        FRACTAL_TASKS_DIR / ".fractal/fractal-tasks-mock0.0.1"
    )

    if FRACTAL_TASKS_MOCK_DIR.exists():
        # If tasks were already collected within this session, re-use
        # collection data
        logging.warning("Manual task collection")
        collection_json = FRACTAL_TASKS_MOCK_DIR / COLLECTION_FILENAME
        with collection_json.open("r") as f:
            collection_data = json.load(f)
        task_collection = TaskCollectStatusV2(**collection_data)
        _insert_tasks(
            task_list=[
                TaskCreateV2(
                    **task.dict(
                        exclude={"id", "owner", "type"},
                        exclude_none=True,
                        exclude_unset=True,
                    )
                )
                for task in task_collection.task_list
            ],
            db=db_sync,
        )
        return "manual_collection"

    else:
        # If tasks were never collected within this session, perform
        # a full API-based task collection
        logging.warning("Actual task collection")

        async with MockCurrentUser(user_kwargs={"is_verified": True}):
            res = await client.post(
                f"{PREFIX}/task/collect/pip/",
                json=dict(
                    package=(
                        testdata_path.parent
                        / "v2/fractal_tasks_mock/dist"
                        / "fractal_tasks_mock-0.0.1-py3-none-any.whl"
                    ).as_posix(),
                    python_version="3.9",
                ),
            )
            state_id = res.json()["id"]
            res = await client.get(f"{PREFIX}/task/collect/{state_id}/")
            assert res.status_code == 200
            data = res.json()["data"]
            if data["status"] != "OK":
                debug(data)
                raise ValueError(
                    "Task collection failed in fractal_tasks_mock."
                )
        return "API_collection"


@pytest.fixture(scope="function")
def relink_python_interpreter_v2(
    tmp_path_factory, fractal_tasks_mock, testdata_path
):
    """
    Rewire python executable in tasks
    """
    import os
    import json
    from pathlib import Path
    from fractal_server.app.schemas.v2.task_collection import (
        TaskCollectStatusV2,
    )

    import logging
    from fractal_server.tasks.utils import COLLECTION_FILENAME
    from .fixtures_slurm import HAS_LOCAL_SBATCH

    if not HAS_LOCAL_SBATCH:

        logger = logging.getLogger("RELINK")
        logger.setLevel(logging.INFO)

        # Identify task Python
        basetemp = tmp_path_factory.getbasetemp()
        FRACTAL_TASKS_DIR = basetemp / "FRACTAL_TASKS_DIR"
        FRACTAL_TASKS_MOCK_DIR = (
            FRACTAL_TASKS_DIR / ".fractal/fractal-tasks-mock0.0.1"
        )
        collection_json = FRACTAL_TASKS_MOCK_DIR / COLLECTION_FILENAME
        with collection_json.open("r") as f:
            collection_data = json.load(f)
        task_collection = TaskCollectStatusV2(**collection_data)
        task_python = Path(
            task_collection.task_list[0].command_non_parallel.split()[0]
        )
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
