from pathlib import Path

import pytest
from devtools import debug

from fractal_server.app.models import TaskGroupV2
from fractal_server.app.schemas.v2 import ResourceType
from fractal_server.ssh._fabric import FractalSSHList
from fractal_server.tasks.config import TasksApptainerSettings
from fractal_server.tasks.config._slurm import SLURMConfig
from fractal_server.tasks.utils import TASK_GROUP_ID_FILENAME
from fractal_server.tasks.v2.ssh.collect_apptainer import SIF_FILENAME


@pytest.mark.container
@pytest.mark.ssh
@pytest.mark.fails_on_macos
async def test_task_collection_apptainer_ssh(
    db,
    app,
    client,
    MockCurrentUser,
    override_settings_factory,
    fractal_ssh_list: FractalSSHList,
    pixi_pkg_targz: Path,
    slurm_ssh_resource_profile_db,
):
    resource, profile = slurm_ssh_resource_profile_db
    credentials = dict(
        host=resource.host,
        user=profile.username,
        key_path=profile.ssh_key_path,
    )

    resource.tasks_apptainer_config = TasksApptainerSettings(
        pixi_version="0.84.1",
        apptainer_bin="/usr/bin/apptainer",
        SLURM_CONFIG=SLURMConfig(
            partition="main",
            cpus=1,
            mem="1G",
            time="60",
        ),
    ).model_dump()
    db.add(resource)
    await db.commit()
    await db.refresh(resource)

    # Assign FractalSSH object to app state
    app.state.fractal_ssh_list = fractal_ssh_list
    fractal_ssh = fractal_ssh_list.get(**credentials)

    override_settings_factory(FRACTAL_RUNNER_BACKEND=ResourceType.SLURM_SSH)

    with pixi_pkg_targz.open("rb") as f:
        files = {
            "file": (
                pixi_pkg_targz.name,
                f.read(),
                "application/gzip",
            )
        }

    async with MockCurrentUser(is_verified=True, profile_id=profile.id) as user:
        # 1 / Failed collection - remote folder already exists
        task_group_path = (
            Path(profile.tasks_remote_dir)
            / str(user.id)
            / "mock-pixi-tasks"
            / "1.2.3"
        ).as_posix()
        fractal_ssh.mkdir(folder=task_group_path, parents=True)
        res = await client.post(
            "api/v2/task/collect/apptainer/",
            data={},
            files=files,
        )
        assert res.status_code == 202
        assert res.json()["status"] == "pending"
        activity_id = res.json()["id"]
        res = await client.get(f"/api/v2/task-group/activity/{activity_id}/")
        assert res.status_code == 200
        activity = res.json()
        assert activity["status"] == "failed"
        assert "already exists" in activity["log"]
        fractal_ssh.remove_folder(folder=task_group_path)

        # 2 / Successful collection
        res = await client.post(
            "api/v2/task/collect/apptainer/",
            data={},
            files=files,
        )
        assert res.status_code == 202
        activity_id = res.json()["id"]
        res = await client.get(f"/api/v2/task-group/activity/{activity_id}/")
        assert res.status_code == 200
        activity = res.json()
        debug(activity["log"])
        assert activity["status"] == "OK"
        assert activity["timestamp_ended"] is not None
        task_group_id = activity["taskgroupv2_id"]

        db.expunge_all()
        task_group = await db.get(TaskGroupV2, task_group_id)
        assert task_group.env_info is not None
        assert len(task_group.task_list) == 1
        assert Path(task_group.path, SIF_FILENAME).exists()
        txt_file = Path(task_group.path, TASK_GROUP_ID_FILENAME)
        assert txt_file.read_text() == str(task_group.id)

        # Task command runs Python inside the container
        task = task_group.task_list[0]
        debug(task.command_non_parallel, task.command_parallel)
        command = task.command_non_parallel or task.command_parallel
        assert command.startswith(
            Path(task_group.path, "project_python.sh").as_posix()
        )
        assert " /opt/" in command

        # 3 / Failed collection - due to non-duplication constraint
        res = await client.post(
            "api/v2/task/collect/apptainer/",
            data={},
            files=files,
        )
        assert res.status_code == 422
        assert "already owns a task group" in str(res.json()["detail"])
