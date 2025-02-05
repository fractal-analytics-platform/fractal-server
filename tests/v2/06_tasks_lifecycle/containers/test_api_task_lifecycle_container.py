import shutil
from pathlib import Path

from devtools import debug
from packaging.version import Version

from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.config import get_settings
from fractal_server.syringe import Inject
from tests.fixtures_slurm import SLURM_USER

settings = Inject(get_settings)


async def test_lifecycle(
    client,
    MockCurrentUser,
    db,
    testdata_path,
    override_settings_factory,
    app,
    tmp777_path: Path,
    request,
    current_py_version,
):
    overrides = dict(FRACTAL_RUNNER_BACKEND="slurm_ssh")
    current_py_version_underscore = current_py_version.replace(".", "_")
    python_key = f"FRACTAL_TASKS_PYTHON_{current_py_version_underscore}"
    python_value = f"/.venv{current_py_version}/bin/python{current_py_version}"
    overrides[python_key] = python_value
    override_settings_factory(**overrides)

    app.state.fractal_ssh_list = request.getfixturevalue("fractal_ssh_list")
    slurmlogin_ip = request.getfixturevalue("slurmlogin_ip")
    ssh_keys = request.getfixturevalue("ssh_keys")
    user_settings_dict = dict(
        ssh_host=slurmlogin_ip,
        ssh_username=SLURM_USER,
        ssh_private_key_path=ssh_keys["private"],
        ssh_tasks_dir=(tmp777_path / "tasks").as_posix(),
        ssh_jobs_dir=(tmp777_path / "artifacts").as_posix(),
    )

    # Absolute path to wheel file (use a path in tmp77_path, so that it is
    # also accessible on the SSH remote host)
    old_wheel_path = (
        testdata_path.parent
        / "v2/fractal_tasks_mock/dist"
        / "fractal_tasks_mock-0.0.1-py3-none-any.whl"
    )
    wheel_path = tmp777_path / old_wheel_path.name
    shutil.copy(old_wheel_path, wheel_path)
    with open(wheel_path, "rb") as f:
        files = {"file": (wheel_path.name, f.read(), "application/zip")}
    async with MockCurrentUser(
        user_kwargs=dict(is_verified=True),
        user_settings_dict=user_settings_dict,
    ):
        # STEP 1: Task collection
        res = await client.post(
            "api/v2/task/collect/pip/",
            data=dict(package_extras="my_extras"),
            files=files,
        )
        assert res.status_code == 202
        assert res.json()["status"] == "pending"
        assert res.json()["log"] is None
        activity = res.json()
        activity_id = activity["id"]
        task_group_id = activity["taskgroupv2_id"]
        res = await client.get(f"/api/v2/task-group/activity/{activity_id}/")
        assert res.status_code == 200
        task_group_activity = res.json()
        assert task_group_activity["status"] == "OK"
        assert task_group_activity["timestamp_ended"] is not None

        log = task_group_activity["log"]
        assert log is not None
        assert log.count("\n") > 0
        assert log.count("\\n") == 0

        task_groupv2_id = task_group_activity["taskgroupv2_id"]
        # Check pip_freeze attribute in TaskGroupV2
        res = await client.get(f"/api/v2/task-group/{task_groupv2_id}/")
        assert res.status_code == 200
        task_group = res.json()
        pip_freeze = task_group["pip_freeze"]
        task_group_wheel_path = task_group["wheel_path"]
        assert (
            f"fractal-tasks-mock @ file://{task_group_wheel_path}"
            in pip_freeze
        )
        pip_version = next(
            line for line in pip_freeze.split("\n") if line.startswith("pip")
        ).split("==")[1]
        assert Version(pip_version) <= Version(
            settings.FRACTAL_MAX_PIP_VERSION
        )
        assert (
            Path(task_group["path"]) / Path(wheel_path).name
        ).as_posix() == (Path(task_group_wheel_path).as_posix())

        # STEP 2: Deactivate task group
        res = await client.post(
            f"api/v2/task-group/{task_group_id}/deactivate/"
        )
        assert res.status_code == 202
        activity_id = res.json()["id"]
        res = await client.get(f"api/v2/task-group/activity/{activity_id}/")
        activity = res.json()
        debug(activity["log"])
        assert res.json()["status"] == "OK"

        # Assertions
        task_group = await db.get(TaskGroupV2, task_group_id)
        assert task_group.active is False
        assert Path(task_group.path).exists()
        assert not Path(task_group.venv_path).exists()
        assert Path(task_group.wheel_path).exists()

        # STEP 3: Reactivate task group
        res = await client.post(
            f"api/v2/task-group/{task_group_id}/reactivate/"
        )
        assert res.status_code == 202
        activity_id = res.json()["id"]
        res = await client.get(f"api/v2/task-group/activity/{activity_id}/")
        activity = res.json()
        debug(activity["log"])
        assert res.json()["status"] == "OK"

        # Assertions
        await db.refresh(task_group)
        assert task_group.active is True
        assert Path(task_group.path).exists()
        assert Path(task_group.venv_path).exists()
        assert Path(task_group.wheel_path).exists()

        # STEP 4: Deactivate a task group created before 2.9.0,
        # which has no pip-freeze information
        task_group.pip_freeze = None
        db.add(task_group)
        await db.commit()
        await db.refresh(task_group)
        res = await client.post(
            f"api/v2/task-group/{task_group_id}/deactivate/"
        )
        assert res.status_code == 202
        activity_id = res.json()["id"]
        res = await client.get(f"api/v2/task-group/activity/{activity_id}/")
        activity = res.json()
        debug(activity["log"])
        assert res.json()["status"] == "OK"

        # Assertions
        db.expunge(task_group)
        task_group = await db.get(TaskGroupV2, task_group_id)
        assert task_group.active is False
        assert task_group.pip_freeze is not None
        assert Path(task_group.path).exists()
        assert not Path(task_group.venv_path).exists()
        assert Path(task_group.wheel_path).exists()
