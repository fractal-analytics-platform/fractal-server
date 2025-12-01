import logging
from pathlib import Path

import pytest
from devtools import debug

from fractal_server.app.models import TaskGroupV2
from fractal_server.app.schemas.v2 import ResourceType
from fractal_server.ssh._fabric import FractalSSH
from fractal_server.ssh._fabric import FractalSSHList

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
async def test_task_collection_ssh_from_pypi(
    db,
    app,
    client,
    MockCurrentUser,
    override_settings_factory,
    fractal_ssh_list: FractalSSHList,
    current_py_version: str,
    slurm_ssh_resource_profile_db,
):
    resource, profile = slurm_ssh_resource_profile_db
    credentials = dict(
        host=resource.host,
        user=profile.username,
        key_path=profile.ssh_key_path,
    )

    assert not fractal_ssh_list.contains(**credentials)
    fractal_ssh = fractal_ssh_list.get(**credentials)

    # Assign FractalSSH object to app state
    app.state.fractal_ssh_list = fractal_ssh_list

    # Override settings with Python/SSH configurations
    override_settings_factory(FRACTAL_RUNNER_BACKEND=ResourceType.SLURM_SSH)

    async with MockCurrentUser(
        user_kwargs=dict(
            is_verified=True,
            profile_id=profile.id,
        ),
    ) as user:
        # SUCCESSFUL COLLECTION
        package_version = "0.1.4"
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            data=dict(
                package="testing-tasks-mock",
                package_version=package_version,
                python_version=current_py_version,
            ),
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
        assert task_group_activity["status"] == "OK"
        task_group_id = task_group_activity["taskgroupv2_id"]
        # Check env_info attribute in TaskGroupV2
        db.expunge_all()
        task_group = await db.get(TaskGroupV2, task_group_id)
        assert f"testing-tasks-mock=={package_version}" in task_group.env_info
        # Check venv_size and venv_file_number in TaskGroupV2
        assert task_group.venv_size_in_kB is not None
        assert task_group.venv_file_number is not None
        # API FAILURE 1, due to non-duplication constraint
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            data=dict(
                package="testing-tasks-mock",
                package_version=package_version,
                python_version=current_py_version,
            ),
        )
        assert res.status_code == 422
        assert "already owns a task group" in str(res.json()["detail"])

        # API FAILURE 2: invalid package name and no version
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            data=dict(
                package="testing-tasks-mock-invalid",
                python_version=current_py_version,
            ),
        )
        assert res.status_code == 422
        debug(res.json())
        expected_error = (
            "Could not get https://pypi.org/simple/testing-tasks-mock-invalid/"
        )
        assert expected_error in str(res.json()["detail"])

        # API FAILURE 3: invalid version
        package_version = "9.9.9"
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            data=dict(
                package="testing-tasks-mock",
                package_version=package_version,
                python_version=current_py_version,
            ),
        )
        assert res.status_code == 422
        assert "No version starting with 9.9.9 found" in res.json()["detail"]
        debug(res.json())

        # BACKGROUND FAILURE 1: existing folder
        package_version = "0.1.2"
        remote_folder = (
            Path(profile.tasks_remote_dir)
            / str(user.id)
            / "testing-tasks-mock"
            / f"{package_version}"
        ).as_posix()
        # Create remote folder
        fractal_ssh.mkdir(folder=remote_folder, parents=True)
        fractal_ssh.run_command(cmd=f"ls {remote_folder}")
        # Run task collection
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            data=dict(
                package="testing-tasks-mock",
                package_version=package_version,
                python_version=current_py_version,
            ),
        )
        assert res.status_code == 202
        task_group_activity_id = res.json()["id"]
        res = await client.get(
            f"/api/v2/task-group/activity/{task_group_activity_id}/"
        )
        assert res.status_code == 200
        task_group_activity = res.json()

        assert task_group_activity["status"] == "failed"
        assert "already exists" in task_group_activity["log"]
        # Check that existing folder was _not_ removed
        fractal_ssh.run_command(cmd=f"ls {remote_folder}")
        # Cleanup: remove folder
        fractal_ssh.remove_folder(
            folder=remote_folder,
            safe_root=profile.tasks_remote_dir,
        )

    _reset_permissions(profile.tasks_remote_dir, fractal_ssh)


@pytest.mark.container
@pytest.mark.ssh
async def test_task_collection_ssh_failure(
    db,
    app,
    client,
    MockCurrentUser,
    override_settings_factory,
    tmp777_path: Path,
    fractal_ssh_list: FractalSSHList,
    current_py_version: str,
    testdata_path,
    monkeypatch,
    slurm_ssh_resource_profile_db,
):
    """
    Test exception handling, including the case where `remove_folder` fails
    _during_ exception handling.
    """
    resource, profile = slurm_ssh_resource_profile_db
    credentials = dict(
        host=resource.host,
        user=profile.username,
        key_path=profile.ssh_key_path,
    )

    assert not fractal_ssh_list.contains(**credentials)
    fractal_ssh = fractal_ssh_list.get(**credentials)

    # Assign FractalSSH object to app state
    app.state.fractal_ssh_list = fractal_ssh_list

    # Override settings with Python/SSH configurations
    override_settings_factory(FRACTAL_RUNNER_BACKEND=ResourceType.SLURM_SSH)

    # Prepare payload that leads to a failed collection
    local_archive_path = (
        testdata_path.parent
        / "v2/fractal_tasks_mock/dist"
        / "fractal_tasks_mock-0.0.1-py3-none-any.whl"
    ).as_posix()
    payload = dict(
        python_version=current_py_version,
    )
    with open(local_archive_path, "rb") as f:
        files = {
            "file": (
                Path(local_archive_path).name,
                f.read(),
                "application/zip",
            )
        }

    async with MockCurrentUser(
        user_kwargs=dict(
            is_verified=True,
            profile_id=profile.id,
        )
    ):
        # Patch ssh.remove_folder
        import fractal_server.tasks.v2.ssh._utils

        ERROR_MSG_1 = "Failed to send file!"
        ERROR_MSG_2 = "Could not remove folder!"

        def patched_send_file(*args, **kwargs):
            raise RuntimeError(ERROR_MSG_1)

        def patched_remove_folder(*args, **kwargs):
            raise RuntimeError(ERROR_MSG_2)

        monkeypatch.setattr(
            fractal_server.tasks.v2.ssh._utils.FractalSSH,
            "send_file",
            patched_send_file,
        )
        monkeypatch.setattr(
            fractal_server.tasks.v2.ssh._utils.FractalSSH,
            "remove_folder",
            patched_remove_folder,
        )
        res = await client.post(
            f"{PREFIX}/collect/pip/", data=payload, files=files
        )
        assert res.status_code == 202
        task_group_activity_id = res.json()["id"]
        res = await client.get(
            f"/api/v2/task-group/activity/{task_group_activity_id}/"
        )
        assert res.status_code == 200
        task_group_activity = res.json()
        assert task_group_activity["status"] == "failed"
        assert ERROR_MSG_1 in task_group_activity["log"]
        assert "Removing folder failed" in task_group_activity["log"]
        assert ERROR_MSG_2 in task_group_activity["log"]

    _reset_permissions(profile.tasks_remote_dir, fractal_ssh)


async def test_task_collection_ssh_failure_no_connection(
    db,
    app,
    client,
    MockCurrentUser,
    current_py_version: str,
    slurm_ssh_resource_profile_fake_db,
):
    """
    Test exception handling for when SSH connection is not available.
    """
    resource, profile = slurm_ssh_resource_profile_fake_db
    # Assign empty FractalSSH object to app state
    app.state.fractal_ssh_list = FractalSSHList()

    async with MockCurrentUser(
        user_kwargs=dict(is_verified=True, profile_id=profile.id)
    ):
        # Trigger task collection
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            data=dict(
                package="testing-tasks-mock",
                python_version=current_py_version,
            ),
        )
        assert res.status_code == 202
        task_group_activity_id = res.json()["id"]
        res = await client.get(
            f"/api/v2/task-group/activity/{task_group_activity_id}/"
        )
        assert res.status_code == 200
        task_group_activity = res.json()
        assert task_group_activity["status"] == "failed"
        assert "Cannot establish SSH connection" in task_group_activity["log"]
