import logging
from pathlib import Path

from devtools import debug

from fractal_server.app.schemas.v2 import CollectionStatusV2
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

    # Override settins with Python/SSH configurations
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
            json=dict(
                package="fractal-tasks-core",
                package_version=package_version,
                python_version=current_py_version,
            ),
        )
        debug(res.json())
        assert res.status_code == 201
        assert res.json()["data"]["status"] == CollectionStatusV2.PENDING
        state_id = res.json()["id"]
        # Get collection info
        res = await client.get(f"{PREFIX}/collect/{state_id}/")
        assert res.status_code == 200
        state_data = res.json()["data"]
        debug(state_data)
        assert state_data["status"] == CollectionStatusV2.OK
        # Check fractal-tasks-core version in freeze data
        assert f"fractal-tasks-core=={package_version}" in state_data["freeze"]
        # Check pip version constraint in freeze data
        pip_version = next(
            line
            for line in state_data["freeze"].split("\n")
            if line.startswith("pip")
        ).split("==")[1]
        assert pip_version == CURRENT_FRACTAL_MAX_PIP_VERSION
        # Check remote venv folder exists
        remote_folder = state_data["venv_path"]
        fractal_ssh.run_command(cmd=f"ls {remote_folder}")

        # API FAILURE 1, due to non-duplication constraint
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            json=dict(
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
            json=dict(
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
            json=dict(
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
            json=dict(
                package="fractal-tasks-core",
                package_version=package_version,
                python_version=current_py_version,
            ),
        )
        assert res.status_code == 201
        state_id = res.json()["id"]
        # Get collection info
        res = await client.get(f"{PREFIX}/collect/{state_id}/")
        assert res.status_code == 200
        state_data = res.json()["data"]
        assert state_data["status"] == CollectionStatusV2.FAIL
        assert "already exists" in state_data["log"]
        # Check that existing folder was _not_ removed
        fractal_ssh.run_command(cmd=f"ls {remote_folder}")
        # Cleanup: remove folder
        fractal_ssh.remove_folder(
            folder=remote_folder,
            safe_root=REMOTE_TASKS_BASE_DIR,
        )

        _reset_permissions(REMOTE_TASKS_BASE_DIR, fractal_ssh)


async def test_task_collection_ssh_from_wheel(
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
    testdata_path: Path,
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

    # Override settins with Python/SSH configurations
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

    # Copy wheel file to remote path
    local_wheel_path = (
        testdata_path.parent
        / "v2/fractal_tasks_mock/dist"
        / "fractal_tasks_mock-0.0.1-py3-none-any.whl"
    ).as_posix()
    remote_wheel_path = (tmp777_path / Path(local_wheel_path).name).as_posix()
    fractal_ssh.send_file(
        local=local_wheel_path,
        remote=remote_wheel_path,
    )

    async with MockCurrentUser(
        user_kwargs=dict(is_verified=True),
        user_settings_dict=user_settings_dict,
    ):
        # SUCCESSFUL COLLECTION
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            json=dict(
                package=remote_wheel_path,
                python_version=current_py_version,
            ),
        )
        assert res.status_code == 201
        assert res.json()["data"]["status"] == CollectionStatusV2.PENDING
        state_id = res.json()["id"]
        # Get collection info
        res = await client.get(f"{PREFIX}/collect/{state_id}/")
        assert res.status_code == 200
        state_data = res.json()["data"]
        debug(state_data["log"])
        assert state_data["status"] == CollectionStatusV2.OK
        # Check remote venv folder exists
        remote_folder = state_data["venv_path"]
        fractal_ssh.run_command(cmd=f"ls {remote_folder}")

        # API FAILURE: wheel file and version set
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            json=dict(
                package=remote_wheel_path,
                package_version="1.2.3",
                python_version=current_py_version,
            ),
        )
        assert res.status_code == 422
        error_msg = (
            "Cannot provide package version when package " "is a wheel file."
        )
        assert error_msg in str(res.json()["detail"])

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

    # Override settins with Python/SSH configurations
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
    remote_wheel_path = (tmp777_path / Path(local_wheel_path).name).as_posix()
    payload = dict(
        package=remote_wheel_path,
        python_version=current_py_version,
    )

    async with MockCurrentUser(
        user_kwargs=dict(is_verified=True),
        user_settings_dict=user_settings_dict,
    ):
        # Trigger task collection (first time)
        res = await client.post(f"{PREFIX}/collect/pip/", json=payload)
        assert res.status_code == 201
        state_id = res.json()["id"]

        # Check that task collection failed
        res = await client.get(f"{PREFIX}/collect/{state_id}/")
        assert res.status_code == 200
        state_data = res.json()["data"]
        assert state_data["status"] == CollectionStatusV2.FAIL
        debug(state_data["log"])
        assert "No such file or directory" in state_data["log"]

        # Patch ssh.remove_folder
        import fractal_server.tasks.v2.collection_ssh

        ERROR_MSG = "Could not remove folder!"

        def patched_remove_folder(*args, **kwargs):
            raise RuntimeError(ERROR_MSG)

        monkeypatch.setattr(
            fractal_server.tasks.v2.collection_ssh.FractalSSH,
            "remove_folder",
            patched_remove_folder,
        )

        # Trigger task collection (first time)
        res = await client.post(f"{PREFIX}/collect/pip/", json=payload)
        assert res.status_code == 201
        state_id = res.json()["id"]

        # Check that task collection failed
        res = await client.get(f"{PREFIX}/collect/{state_id}/")
        assert res.status_code == 200
        state_data = res.json()["data"]
        assert state_data["status"] == CollectionStatusV2.FAIL
        debug(state_data["log"])
        assert "Removing folder failed" in state_data["log"]
        assert ERROR_MSG in state_data["log"]

        _reset_permissions(REMOTE_TASKS_BASE_DIR, fractal_ssh)
