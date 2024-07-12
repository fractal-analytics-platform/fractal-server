import logging
import sys
from pathlib import Path

from devtools import debug  # noqa

from fractal_server.app.schemas.v2 import CollectionStatusV2
from fractal_server.config import get_settings
from fractal_server.ssh._fabric import FractalSSH
from fractal_server.syringe import Inject


PREFIX = "api/v2/task"
INFO = sys.version_info
CURRENT_PYTHON = f"{INFO.major}.{INFO.minor}"


async def test_task_collection_ssh_from_pypi(
    db,
    app,
    client,
    MockCurrentUser,
    override_settings_factory,
    tmp_path: Path,
    fractal_ssh: FractalSSH,
):
    # Note 1: Use function-scoped `FRACTAL_TASKS_DIR` to avoid sharing state.
    # Note 2: Set logging level to CRITICAL, and then make sure that
    # task-collection logs are included
    override_settings_factory(
        FRACTAL_TASKS_DIR=(tmp_path / "FRACTAL_TASKS_DIR"),
        FRACTAL_LOGGING_LEVEL=logging.CRITICAL,
        FRACTAL_TASKS_PYTHON_DEFAULT_VERSION=CURRENT_PYTHON,
        FRACTAL_RUNNER_BACKEND="slurm_ssh",
        FRACTAL_SLURM_SSH_WORKING_BASE_DIR="/tmp/SLURM_SSH_WORKING_BASE_DIR",
    )
    settings = Inject(get_settings)

    app.state.fractal_ssh = fractal_ssh

    # Prepare and validate payload
    PACKAGE_VERSION = "1.0.2"
    PYTHON_VERSION = settings.FRACTAL_TASKS_PYTHON_DEFAULT_VERSION
    payload = dict(
        package="fractal-tasks-core",
        package_version=PACKAGE_VERSION,
        python_version=PYTHON_VERSION,
    )
    debug(payload)

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        # Trigger task collection
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            json=payload,
        )
        assert res.status_code == 201
        assert res.json()["data"]["status"] == CollectionStatusV2.PENDING
        state = res.json()
        state_id = state["id"]

        # Get collection info
        res = await client.get(f"{PREFIX}/collect/{state_id}/")
        assert res.status_code == 200
        state = res.json()
        data = state["data"]
        debug(data)
