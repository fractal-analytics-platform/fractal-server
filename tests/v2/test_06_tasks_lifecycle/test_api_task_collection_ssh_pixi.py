import logging
from pathlib import Path

import pytest
from devtools import debug

from fractal_server.app.models import TaskGroupV2
from fractal_server.config import PixiSettings
from fractal_server.ssh._fabric import FractalSSH
from fractal_server.ssh._fabric import FractalSSHList
from tests.fixtures_slurm import SLURM_USER

PREFIX = "api/v2/task"


def _reset_permissions(remote_folder: str, fractal_ssh: FractalSSH):
    """
    This is useful to avoid "garbage" folders (in pytest tmp folder) that
    cannot be removed because of wrong permissions.
    """
    logging.warning(f"[_reset_permissions] {remote_folder=}")
    fractal_ssh.run_command(cmd=f"chmod -R 777 {remote_folder}")


@pytest.mark.container
@pytest.mark.ssh
async def test_task_collection_ssh_pixi(
    db,
    app,
    client,
    MockCurrentUser,
    override_settings_factory,
    tmp777_path: Path,
    fractal_ssh_list: FractalSSHList,
    slurmlogin_ip,
    ssh_keys,
    pixi_pkg_targz: Path,
):
    credentials = dict(
        host=slurmlogin_ip,
        user=SLURM_USER,
        key_path=ssh_keys["private"],
    )

    assert not fractal_ssh_list.contains(**credentials)

    # Define and create remote working directory
    REMOTE_TASKS_BASE_DIR = (tmp777_path / "tasks").as_posix()

    # Assign FractalSSH object to app state
    app.state.fractal_ssh_list = fractal_ssh_list

    override_settings_factory(
        FRACTAL_RUNNER_BACKEND="slurm_ssh",
        FRACTAL_PIXI_CONFIG_FILE="fake",
        pixi=PixiSettings(
            default_version="0.47.0",
            versions={
                "0.47.0": "/pixi/0.47.0",
            },
        ),
    )

    user_settings_dict = dict(
        ssh_host=slurmlogin_ip,
        ssh_username=SLURM_USER,
        ssh_private_key_path=ssh_keys["private"],
        ssh_tasks_dir=REMOTE_TASKS_BASE_DIR,
        ssh_jobs_dir=(tmp777_path / "jobs").as_posix(),
    )

    with pixi_pkg_targz.open("rb") as f:
        files = {
            "file": (
                pixi_pkg_targz.name,
                f.read(),
                "application/gzip",
            )
        }

    async with MockCurrentUser(
        user_kwargs=dict(is_verified=True),
        user_settings_dict=user_settings_dict,
    ):
        # SUCCESSFUL COLLECTION
        res = await client.post(
            f"{PREFIX}/collect/pixi/",
            data={},
            files=files,
        )
        debug(res.json())
        assert res.status_code == 202
        assert res.json()["status"] == "pending"
        task_group_activity_id = res.json()["id"]
        res = await client.get(
            f"/api/v2/task-group/activity/{task_group_activity_id}/"
        )
        assert res.status_code == 200
        task_group_activity = res.json()
        debug(res.json())
        return
        assert task_group_activity["status"] == "OK"
        task_groupv2_id = task_group_activity["taskgroupv2_id"]
        # Check env_info attribute in TaskGroupV2
        db.expunge_all()
        task_group = await db.get(TaskGroupV2, task_groupv2_id)
        # FIXME: Add assertion?
        # Check venv_size and venv_file_number in TaskGroupV2
        assert task_group.venv_size_in_kB is not None
        assert task_group.venv_file_number is not None
        # API FAILURE 1, due to non-duplication constraint
        res = await client.post(
            f"{PREFIX}/collect/pixi/",
            data={},
            files=files,
        )
        assert res.status_code == 422
        assert "already owns a task group" in str(res.json()["detail"])
