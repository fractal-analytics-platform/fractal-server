import logging
import os
import shlex
import subprocess  # nosec
from pathlib import Path

import pytest
from devtools import debug

from fractal_server.app.models import TaskGroupV2
from fractal_server.config import PixiSettings
from fractal_server.ssh._fabric import FractalSSH
from fractal_server.ssh._fabric import FractalSSHList
from fractal_server.tasks.v2.utils_pixi import SOURCE_DIR_NAME
from tests.fixtures_slurm import SLURM_USER


def _reset_permissions(remote_folder: str, fractal_ssh: FractalSSH):
    """
    This is useful to avoid "garbage" folders (in pytest tmp folder) that
    cannot be removed because of wrong permissions.
    """
    logging.warning(f"[_reset_permissions] {remote_folder=}")
    fractal_ssh.run_command(cmd=f"chmod -R 777 {remote_folder}")


@pytest.fixture(scope="function")
def pixi_ssh(tmp777_path: Path) -> PixiSettings:
    """
    Similar to the `pixi` fixture, but it uses a 777 pixi folder, which
    is also writeable from within SSH remote-host container.
    """
    pixi_common = tmp777_path / "pixi"
    original_umask = os.umask(0)
    pixi_common.mkdir(mode=0o777)
    os.umask(original_umask)
    pixi_home = pixi_common / "0.47.0"
    script_contents = (
        "export PIXI_NO_PATH_UPDATE=1\n"
        "export PIXI_VERSION=0.47.0\n"
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

    return PixiSettings(
        default_version="0.47.0",
        versions={"0.47.0": pixi_home.as_posix()},
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
    slurmlogin_ip,
    ssh_keys,
    pixi_ssh: PixiSettings,
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
    fractal_ssh = fractal_ssh_list.get(**credentials)

    override_settings_factory(
        FRACTAL_RUNNER_BACKEND="slurm_ssh",
        FRACTAL_PIXI_CONFIG_FILE="fake",
        pixi=pixi_ssh,
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
    ) as user:
        # 1 / Failed collection - remote folder already exists
        task_group_path = (
            Path(REMOTE_TASKS_BASE_DIR)
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
            safe_root=REMOTE_TASKS_BASE_DIR,
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
