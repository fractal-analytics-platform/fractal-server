import logging
from pathlib import Path

from devtools import debug

from fractal_server.ssh._fabric import FractalSSH
from fractal_server.ssh._fabric import FractalSSHList
from tests.fixtures_slurm import SLURM_USER

PREFIX = "api/v2/task"

CURRENT_FRACTAL_MAX_PIP_VERSION = "24.0"


def _reset_permissions(remote_folder: str, fractal_ssh: FractalSSH):
    """
    This is useful to avoid "garbage" folders (in pytest tmp folder) that
    cannot be removed because of wrong permissions.
    """
    logging.warning(f"[_reset_permissions] {remote_folder=}")
    fractal_ssh.run_command(cmd=f"chmod -R 777 {remote_folder}")


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
    REMOTE_TASKS_BASE_DIR = (tmp777_path / "tasks").as_posix()

    # Assign FractalSSH object to app state
    app.state.fractal_ssh_list = fractal_ssh_list

    # Override settings with Python/SSH configurations
    current_py_version_underscore = current_py_version.replace(".", "_")
    PY_KEY = f"FRACTAL_TASKS_PYTHON_{current_py_version_underscore}"
    settings_overrides = {
        "FRACTAL_TASKS_PYTHON_DEFAULT_VERSION": current_py_version,
        PY_KEY: f"/.venv{current_py_version}/bin/python{current_py_version}",
        "FRACTAL_RUNNER_BACKEND": "slurm_ssh",
        "FRACTAL_MAX_PIP_VERSION": CURRENT_FRACTAL_MAX_PIP_VERSION,
    }
    override_settings_factory(**settings_overrides)

    user_settings_dict = dict(
        ssh_host=slurmlogin_ip,
        ssh_username=SLURM_USER,
        ssh_private_key_path=ssh_keys["private"],
        ssh_tasks_dir=REMOTE_TASKS_BASE_DIR,
        ssh_jobs_dir=(tmp777_path / "jobs").as_posix(),
    )

    async with MockCurrentUser(
        user_kwargs=dict(is_verified=True),
        user_settings_dict=user_settings_dict,
    ) as user:
        # SUCCESSFUL COLLECTION
        package_version = "1.3.2"
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            data=dict(
                package="fractal-tasks-core",
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
        task_groupv2_id = task_group_activity["taskgroupv2_id"]
        # Check pip_freeze attribute in TaskGroupV2
        res = await client.get("/api/v2/task-group/" f"{task_groupv2_id}/")
        assert res.status_code == 200
        task_group = res.json()
        assert (
            f"fractal-tasks-core=={package_version}"
            in task_group["pip_freeze"]
        )
        # Check venv_size and venv_file_number in TaskGroupV2
        assert task_group["venv_size_in_kB"] is not None
        assert task_group["venv_file_number"] is not None
        # API FAILURE 1, due to non-duplication constraint
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            data=dict(
                package="fractal-tasks-core",
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
                package="fractal-tasks-core-invalid",
                python_version=current_py_version,
            ),
        )
        assert res.status_code == 422
        debug(res.json())
        expected_error = (
            "Could not get https://pypi.org/pypi/"
            "fractal-tasks-core-invalid/json"
        )
        assert expected_error in str(res.json()["detail"])

        # API FAILURE 3: invalid version
        package_version = "9.9.9"
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            data=dict(
                package="fractal-tasks-core",
                package_version=package_version,
                python_version=current_py_version,
            ),
        )
        assert res.status_code == 422
        assert "No version starting with 9.9.9 found" in res.json()["detail"]
        debug(res.json())

        # BACKGROUND FAILURE 1: existing folder
        package_version = "1.2.0"
        remote_folder = (
            Path(REMOTE_TASKS_BASE_DIR)
            / str(user.id)
            / "fractal-tasks-core"
            / f"{package_version}"
        ).as_posix()
        # Create remote folder
        fractal_ssh.mkdir(folder=remote_folder, parents=True)
        fractal_ssh.run_command(cmd=f"ls {remote_folder}")
        # Run task collection
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            data=dict(
                package="fractal-tasks-core",
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
            safe_root=REMOTE_TASKS_BASE_DIR,
        )

        _reset_permissions(REMOTE_TASKS_BASE_DIR, fractal_ssh)


async def test_task_collection_ssh_failure(
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
    testdata_path,
    monkeypatch,
):
    """
    Test exception handling, including the case where `remove_folder` fails
    _during_ exception handling.
    """

    credentials = dict(
        host=slurmlogin_ip,
        user=SLURM_USER,
        key_path=ssh_keys["private"],
    )

    assert not fractal_ssh_list.contains(**credentials)
    fractal_ssh = fractal_ssh_list.get(**credentials)

    # Define and create remote working directory
    REMOTE_TASKS_BASE_DIR = (tmp777_path / "tasks").as_posix()

    # Assign FractalSSH object to app state
    app.state.fractal_ssh_list = fractal_ssh_list

    # Override settings with Python/SSH configurations
    current_py_version_underscore = current_py_version.replace(".", "_")
    PY_KEY = f"FRACTAL_TASKS_PYTHON_{current_py_version_underscore}"
    settings_overrides = {
        "FRACTAL_TASKS_PYTHON_DEFAULT_VERSION": current_py_version,
        PY_KEY: f"/usr/bin/python{current_py_version}",
        "FRACTAL_RUNNER_BACKEND": "slurm_ssh",
        "FRACTAL_MAX_PIP_VERSION": CURRENT_FRACTAL_MAX_PIP_VERSION,
    }
    override_settings_factory(**settings_overrides)

    user_settings_dict = dict(
        ssh_host=slurmlogin_ip,
        ssh_username=SLURM_USER,
        ssh_private_key_path=ssh_keys["private"],
        ssh_tasks_dir=REMOTE_TASKS_BASE_DIR,
        ssh_jobs_dir=(tmp777_path / "jobs").as_posix(),
    )

    # Prepare payload that leads to a failed collection
    local_wheel_path = (
        testdata_path.parent
        / "v2/fractal_tasks_mock/dist"
        / "fractal_tasks_mock-0.0.1-py3-none-any.whl"
    ).as_posix()
    payload = dict(
        python_version=current_py_version,
    )
    with open(local_wheel_path, "rb") as f:
        files = {
            "file": (Path(local_wheel_path).name, f.read(), "application/zip")
        }

    async with MockCurrentUser(
        user_kwargs=dict(is_verified=True),
        user_settings_dict=user_settings_dict,
    ):
        # Patch ssh.remove_folder
        import fractal_server.tasks.v2.ssh.collect

        ERROR_MSG_1 = "Failed to send file!"
        ERROR_MSG_2 = "Could not remove folder!"

        def patched_send_file(*args, **kwargs):
            raise RuntimeError(ERROR_MSG_1)

        def patched_remove_folder(*args, **kwargs):
            raise RuntimeError(ERROR_MSG_2)

        monkeypatch.setattr(
            fractal_server.tasks.v2.ssh.collect.FractalSSH,
            "remove_folder",
            patched_remove_folder,
        )
        monkeypatch.setattr(
            fractal_server.tasks.v2.ssh.collect.FractalSSH,
            "send_file",
            patched_send_file,
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

        _reset_permissions(REMOTE_TASKS_BASE_DIR, fractal_ssh)


async def test_task_collection_ssh_failure_no_connection(
    db,
    app,
    client,
    MockCurrentUser,
    override_settings_factory,
    current_py_version: str,
):
    """
    Test exception handling for when SSH connection is not available.
    """

    # Assign empty FractalSSH object to app state
    app.state.fractal_ssh_list = FractalSSHList()

    # Override settings with Python/SSH configurations
    current_py_version_underscore = current_py_version.replace(".", "_")
    PY_KEY = f"FRACTAL_TASKS_PYTHON_{current_py_version_underscore}"
    settings_overrides = {
        "FRACTAL_TASKS_PYTHON_DEFAULT_VERSION": current_py_version,
        PY_KEY: f"/usr/bin/python{current_py_version}",
        "FRACTAL_RUNNER_BACKEND": "slurm_ssh",
    }
    override_settings_factory(**settings_overrides)

    user_settings_dict = dict(
        ssh_host="fake",
        ssh_username="fake",
        ssh_private_key_path="fake",
        ssh_tasks_dir="/fake",
        ssh_jobs_dir="/fake",
    )

    async with MockCurrentUser(
        user_kwargs=dict(is_verified=True),
        user_settings_dict=user_settings_dict,
    ):
        # Trigger task collection
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            data=dict(
                package="fractal-tasks-core",
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
