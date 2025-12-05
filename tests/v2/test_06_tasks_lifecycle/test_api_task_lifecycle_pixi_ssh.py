import logging
import os
import shlex
import subprocess  # nosec
from pathlib import Path

import pytest
from devtools import debug

from fractal_server.app.models import TaskGroupV2
from fractal_server.app.schemas.v2 import ResourceType
from fractal_server.ssh._fabric import FractalSSH
from fractal_server.ssh._fabric import FractalSSHList
from fractal_server.tasks.config import PixiSLURMConfig
from fractal_server.tasks.config import TasksPixiSettings
from fractal_server.tasks.v2.utils_pixi import SOURCE_DIR_NAME


def _reset_permissions(remote_folder: str, fractal_ssh: FractalSSH):
    """
    This is useful to avoid "garbage" folders (in pytest tmp folder) that
    cannot be removed because of wrong permissions.
    """
    logging.warning(f"[_reset_permissions] {remote_folder=}")
    fractal_ssh.run_command(cmd=f"chmod -R 777 {remote_folder}")


@pytest.fixture(scope="function")
def pixi_ssh(tmp777_path: Path) -> TasksPixiSettings:
    """
    Similar to the `pixi` fixture, but it uses a 777 pixi folder, which
    is also writeable from within SSH remote-host container.
    """
    pixi_common = tmp777_path / "pixi"
    original_umask = os.umask(0)
    pixi_common.mkdir(mode=0o777)
    os.umask(original_umask)
    pixi_home = pixi_common / "0.54.1"
    script_contents = (
        "export PIXI_NO_PATH_UPDATE=1\n"
        "export PIXI_VERSION=0.54.1\n"
        f"export PIXI_HOME={pixi_home.as_posix()}\n"
        "curl -fsSL https://pixi.sh/install.sh | sh\n"
        f"chmod -R 777 {pixi_home.as_posix()}\n"
    )
    script_path = pixi_common / "install_pixi.sh"
    with script_path.open("w") as f:
        f.write(script_contents)
    cmd = f"bash {script_path.as_posix()}"
    logging.info(f"START running {cmd=}")
    subprocess.run(  # nosec
        shlex.split(cmd), capture_output=True, encoding="utf8", check=True
    )
    logging.info(f"END   running {cmd=}")

    return TasksPixiSettings(
        default_version="0.54.1",
        versions={"0.54.1": pixi_home.as_posix()},
        SLURM_CONFIG=PixiSLURMConfig(
            partition="main",
            cpus=1,
            mem="1G",
            time="60",
        ),
    )


@pytest.mark.container
@pytest.mark.ssh
@pytest.mark.fails_on_macos
async def test_task_group_lifecycle_pixi_ssh(
    db,
    app,
    client,
    MockCurrentUser,
    override_settings_factory,
    tmp777_path: Path,
    fractal_ssh_list: FractalSSHList,
    pixi_ssh: TasksPixiSettings,
    pixi_pkg_targz: Path,
    slurm_ssh_resource_profile_db,
):
    resource, profile = slurm_ssh_resource_profile_db
    credentials = dict(
        host=resource.host,
        user=profile.username,
        key_path=profile.ssh_key_path,
    )

    resource.tasks_pixi_config = pixi_ssh.model_dump()
    db.add(resource)
    await db.commit()
    await db.refresh(resource)

    assert not fractal_ssh_list.contains(**credentials)

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
            / "0.2.1"
        ).as_posix()
        fractal_ssh.mkdir(folder=task_group_path, parents=True)
        res = await client.post(
            "api/v2/task/collect/pixi/",
            data={},
            files=files,
        )
        debug(res.json())
        assert res.status_code == 202
        assert res.json()["status"] == "pending"
        activity_id = res.json()["id"]
        res = await client.get(f"/api/v2/task-group/activity/{activity_id}/")
        assert res.status_code == 200
        activity = res.json()
        assert activity["log"] is not None
        assert activity["timestamp_ended"] is not None
        assert activity["status"] == "failed"
        assert "already exists" in activity["log"]
        fractal_ssh.remove_folder(
            folder=task_group_path,
            safe_root=profile.tasks_remote_dir,
        )

        # 2 / Successful collection
        res = await client.post(
            "api/v2/task/collect/pixi/",
            data={},
            files=files,
        )
        debug(res.json())
        assert res.status_code == 202
        assert res.json()["status"] == "pending"
        activity_id = res.json()["id"]
        res = await client.get(f"/api/v2/task-group/activity/{activity_id}/")
        assert res.status_code == 200
        activity = res.json()
        debug(activity["log"])
        assert activity["log"] is not None
        assert activity["timestamp_ended"] is not None
        assert activity["status"] == "OK"
        task_group_id = activity["taskgroupv2_id"]
        res = await client.get(f"/api/v2/task-group/{task_group_id}/")

        # Check `TaskGroupV2.env_info` (note: it is not part of the
        # API response)
        db.expunge_all()
        task_group = await db.get(TaskGroupV2, task_group_id)
        assert len(task_group.task_list) == 1
        assert task_group.venv_size_in_kB is not None
        assert task_group.venv_file_number is not None
        assert task_group.env_info is not None

        # 3 / Failed collection - due to non-duplication constraint
        res = await client.post(
            "api/v2/task/collect/pixi/",
            data={},
            files=files,
        )
        assert res.status_code == 422
        assert "already owns a task group" in str(res.json()["detail"])

        # 4 / Successful deactivation
        res = await client.post(
            f"/api/v2/task-group/{task_group_id}/deactivate/",
            data={},
        )
        assert res.status_code == 202
        activity_id = res.json()["id"]
        res = await client.get(f"/api/v2/task-group/activity/{activity_id}/")
        assert res.status_code == 200
        activity = res.json()
        assert activity["status"] == "OK"
        assert Path(task_group.archive_path).exists()
        assert not Path(task_group.path, SOURCE_DIR_NAME).exists()

        # 5 / Failed deactivation (folder does not exist)
        db.expunge_all()
        task_group.active = True  # mock an active task group
        await db.merge(task_group)
        await db.commit()
        res = await client.post(
            f"/api/v2/task-group/{task_group_id}/deactivate/",
            data={},
        )
        assert res.status_code == 202
        activity_id = res.json()["id"]
        res = await client.get(f"/api/v2/task-group/activity/{activity_id}/")
        assert res.status_code == 200
        activity = res.json()
        assert activity["status"] == "failed"
        db.expunge_all()
        task_group.active = False  # restore `active=False`
        await db.merge(task_group)
        await db.commit()

        # 6 / Failed reactivation - (fake) folder already exists
        fake_remote_dir = Path(task_group.path, SOURCE_DIR_NAME).as_posix()
        fractal_ssh.mkdir(folder=fake_remote_dir)  # Create fake folder
        res = await client.post(
            f"/api/v2/task-group/{task_group_id}/reactivate/",
            data={},
        )
        assert res.status_code == 202
        activity_id = res.json()["id"]
        res = await client.get(f"/api/v2/task-group/activity/{activity_id}/")
        assert res.status_code == 200
        activity = res.json()
        debug(activity)
        assert activity["status"] == "failed"
        fractal_ssh.remove_folder(  # Remove fake folder
            folder=fake_remote_dir,
            safe_root=task_group.path,
        )

        # 7 / Successful reactivation
        res = await client.post(
            f"/api/v2/task-group/{task_group_id}/reactivate/",
            data={},
        )
        assert res.status_code == 202
        activity_id = res.json()["id"]
        res = await client.get(f"/api/v2/task-group/activity/{activity_id}/")
        assert res.status_code == 200
        activity = res.json()
        debug(activity)
        assert activity["status"] == "OK"
