from pathlib import Path

import pytest

from fractal_server.app.schemas.v2 import CollectionStatusV2
from fractal_server.ssh._fabric import FractalSSHList
from tests.fixtures_slurm import SLURM_USER

PREFIX = "api/v2/task"


async def test_task_collection_ssh_from_pypi(
    db,
    app,
    client,
    MockCurrentUser,
    override_settings_factory,
    tmp777_path: Path,
    fractal_ssh_list: FractalSSHList,
    current_py_version: str,
    slurmlogin_ip,
    ssh_keys,
):

    credentials = dict(
        host=slurmlogin_ip,
        user=SLURM_USER,
        key_path=ssh_keys["private"],
    )

    assert not fractal_ssh_list.contains(**credentials)
    fractal_ssh = fractal_ssh_list.get(**credentials)

    # Define and create remote working directory
    TASKS_BASE_DIR = (tmp777_path / "tasks").as_posix()
    fractal_ssh.mkdir(folder=TASKS_BASE_DIR)

    # Assign FractalSSH object to app state
    app.state.fractal_ssh_list = fractal_ssh_list

    # Override settins with Python/SSH configurations
    current_py_version_underscore = current_py_version.replace(".", "_")
    PY_KEY = f"FRACTAL_TASKS_PYTHON_{current_py_version_underscore}"
    settings_overrides = {
        "FRACTAL_TASKS_PYTHON_DEFAULT_VERSION": current_py_version,
        PY_KEY: f"/usr/bin/python{current_py_version}",
        "FRACTAL_RUNNER_BACKEND": "slurm_ssh",
    }
    override_settings_factory(**settings_overrides)

    user_settings_dict = dict(
        ssh_host=slurmlogin_ip,
        ssh_username=SLURM_USER,
        ssh_private_key_path=ssh_keys["private"],
        ssh_tasks_dir=TASKS_BASE_DIR,
        ssh_jobs_dir=(tmp777_path / "jobs").as_posix(),
    )

    async with MockCurrentUser(
        user_kwargs=dict(is_verified=True),
        user_settings_dict=user_settings_dict,
    ) as user:

        # CASE 1: successful collection

        # Trigger task collection
        PACKAGE_VERSION = "1.0.2"
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            json=dict(
                package="fractal-tasks-core",
                package_version=PACKAGE_VERSION,
                python_version=current_py_version,
            ),
        )
        assert res.status_code == 201
        assert res.json()["data"]["status"] == CollectionStatusV2.PENDING
        state_id = res.json()["id"]

        # Get collection info
        res = await client.get(f"{PREFIX}/collect/{state_id}/")
        assert res.status_code == 200
        data = res.json()["data"]
        assert data["status"] == CollectionStatusV2.OK
        assert f"fractal-tasks-core=={PACKAGE_VERSION}" in data["freeze"]
        remote_folder = (
            Path(TASKS_BASE_DIR)
            / str(user.id)
            / "fractal-tasks-core"
            / f"{PACKAGE_VERSION}"
        ).as_posix()
        fractal_ssh.run_command(cmd=f"ls {remote_folder}")

        # CASE 2: Failure due to invalid version

        # Trigger task collection
        PACKAGE_VERSION = "9.9.9"
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            json=dict(
                package="fractal-tasks-core",
                package_version=PACKAGE_VERSION,
                python_version=current_py_version,
            ),
        )
        assert res.status_code == 201
        assert res.json()["data"]["status"] == CollectionStatusV2.PENDING
        state_id = res.json()["id"]

        # Get collection info
        res = await client.get(f"{PREFIX}/collect/{state_id}/")
        assert res.status_code == 200
        data = res.json()["data"]
        assert data["status"] == CollectionStatusV2.FAIL
        assert "No matching distribution found" in data["log"]
        assert f"fractal-tasks-core=={PACKAGE_VERSION}" in data["log"]
        remote_folder = (
            Path(TASKS_BASE_DIR)
            / str(user.id)
            / "fractal-tasks-core"
            / f"{PACKAGE_VERSION}"
        ).as_posix()
        # Check that folder was removed
        with pytest.raises(RuntimeError, match="No such file or directory"):
            fractal_ssh.run_command(cmd=f"ls {remote_folder}")
