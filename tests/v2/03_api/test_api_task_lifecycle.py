import shutil
from pathlib import Path

import pytest
from devtools import debug

from fractal_server.app.models.v2 import JobV2
from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.schemas.v2 import JobStatusTypeV2
from fractal_server.app.schemas.v2 import TaskGroupActivityActionV2
from fractal_server.app.schemas.v2 import TaskGroupActivityStatusV2
from tests.fixtures_slurm import SLURM_USER


class MockFractalSSHList:
    """
    Implement the only method which is used from within the API.
    """

    def get(self, *args, **kwargs):
        return None


@pytest.mark.parametrize("FRACTAL_RUNNER_BACKEND", ["local", "slurm_ssh"])
async def test_deactivate_task_group_api(
    app,
    client,
    MockCurrentUser,
    db,
    task_factory_v2,
    FRACTAL_RUNNER_BACKEND,
    override_settings_factory,
):
    """
    This tests _only_ the API of the `deactivate` endpoint.
    """

    override_settings_factory(
        FRACTAL_RUNNER_BACKEND=FRACTAL_RUNNER_BACKEND,
    )

    async with MockCurrentUser() as different_user:
        non_accessible_task = await task_factory_v2(
            user_id=different_user.id, name="task"
        )

    if FRACTAL_RUNNER_BACKEND == "slurm_ssh":
        app.state.fractal_ssh_list = MockFractalSSHList()
        user_settings_dict = dict(
            ssh_host="ssh_host",
            ssh_username="ssh_username",
            ssh_private_key_path="/invalid/ssh_private_key_path",
            ssh_tasks_dir="/invalid/ssh_tasks_dir",
            ssh_jobs_dir="/invalid/ssh_jobs_dir",
        )
    else:
        user_settings_dict = {}

    async with MockCurrentUser(user_settings_dict=user_settings_dict) as user:
        # Create mock task groups
        non_active_task = await task_factory_v2(
            user_id=user.id, name="task", task_group_kwargs=dict(active=False)
        )
        task_other = await task_factory_v2(
            user_id=user.id,
            version=None,
            name="task",
            task_group_kwargs=dict(origin="other"),
        )
        task_pypi = await task_factory_v2(
            user_id=user.id,
            name="task",
            version="1.2.3",
            task_group_kwargs=dict(
                origin="pypi", venv_path="/invalid/so/it/fails"
            ),
        )

        # API failure: Not full access to another user's task group
        res = await client.post(
            "api/v2/task-group/"
            f"{non_accessible_task.taskgroupv2_id}/deactivate/"
        )
        assert res.status_code == 403

        # API failure: Non-active task group cannot be deactivated
        res = await client.post(
            f"api/v2/task-group/{non_active_task.taskgroupv2_id}/deactivate/"
        )
        assert res.status_code == 422

        # API success with `origin="other"`
        res = await client.post(
            f"api/v2/task-group/{task_other.taskgroupv2_id}/deactivate/"
        )
        activity = res.json()
        assert res.status_code == 202
        assert activity["version"] == "N/A"
        assert activity["status"] == TaskGroupActivityStatusV2.OK
        assert activity["action"] == TaskGroupActivityActionV2.DEACTIVATE
        assert activity["timestamp_started"] is not None
        assert activity["timestamp_ended"] is not None
        task_group_other = await db.get(TaskGroupV2, task_other.taskgroupv2_id)
        assert task_group_other.active is False

        # API success with `origin="pypi"`
        res = await client.post(
            f"api/v2/task-group/{task_pypi.taskgroupv2_id}/deactivate/"
        )
        activity = res.json()
        assert res.status_code == 202
        activity_id = activity["id"]
        assert activity["status"] == TaskGroupActivityStatusV2.PENDING
        assert activity["action"] == TaskGroupActivityActionV2.DEACTIVATE
        assert activity["timestamp_started"] is not None
        assert activity["timestamp_ended"] is None
        task_group_pypi = await db.get(TaskGroupV2, task_pypi.taskgroupv2_id)
        assert activity["version"] == task_group_pypi.version
        assert task_group_pypi.active is False

        # Check that background task failed
        res = await client.get(f"api/v2/task-group/activity/{activity_id}/")
        assert res.json()["status"] == "failed"
        if FRACTAL_RUNNER_BACKEND == "slurm_ssh":
            assert "Cannot establish SSH connection" in res.json()["log"]
        else:
            assert "does not exist" in res.json()["log"]


@pytest.mark.parametrize("FRACTAL_RUNNER_BACKEND", ["local", "slurm_ssh"])
async def test_reactivate_task_group_api(
    app,
    client,
    MockCurrentUser,
    db,
    task_factory_v2,
    current_py_version,
    FRACTAL_RUNNER_BACKEND,
    override_settings_factory,
):
    """
    This tests _only_ the API of the `reactivate` endpoint.
    """

    override_settings_factory(
        FRACTAL_RUNNER_BACKEND=FRACTAL_RUNNER_BACKEND,
    )

    async with MockCurrentUser() as different_user:
        non_accessible_task = await task_factory_v2(
            user_id=different_user.id, name="task"
        )

    if FRACTAL_RUNNER_BACKEND == "slurm_ssh":
        app.state.fractal_ssh_list = MockFractalSSHList()
        user_settings_dict = dict(
            ssh_host="ssh_host",
            ssh_username="ssh_username",
            ssh_private_key_path="/invalid/ssh_private_key_path",
            ssh_tasks_dir="/invalid/ssh_tasks_dir",
            ssh_jobs_dir="/invalid/ssh_jobs_dir",
        )
    else:
        user_settings_dict = {}
    async with MockCurrentUser(user_settings_dict=user_settings_dict) as user:
        # Create mock task groups
        active_task = await task_factory_v2(user_id=user.id, name="task")
        task_other = await task_factory_v2(
            user_id=user.id,
            version=None,
            name="task",
            task_group_kwargs=dict(active=False),
        )
        task_pypi = await task_factory_v2(
            user_id=user.id,
            name="task",
            version="1.2.3",
            task_group_kwargs=dict(
                origin="pypi",
                active=False,
                venv_path="/invalid/so/it/fails",
                python_version=current_py_version,
            ),
        )

        # API failure: Not full access to another user's task group
        res = await client.post(
            "api/v2/task-group/"
            f"{non_accessible_task.taskgroupv2_id}/reactivate/"
        )
        assert res.status_code == 403

        # API failure: Active task group cannot be reactivated
        res = await client.post(
            f"api/v2/task-group/{active_task.taskgroupv2_id}/reactivate/"
        )
        assert res.status_code == 422

        # API success with `origin="other"`
        res = await client.post(
            f"api/v2/task-group/{task_other.taskgroupv2_id}/reactivate/"
        )
        activity = res.json()
        assert res.status_code == 202
        assert activity["version"] == "N/A"
        assert activity["status"] == TaskGroupActivityStatusV2.OK
        assert activity["action"] == TaskGroupActivityActionV2.REACTIVATE
        assert activity["timestamp_started"] is not None
        assert activity["timestamp_ended"] is not None
        task_group_other = await db.get(TaskGroupV2, task_other.taskgroupv2_id)
        assert task_group_other.active is True

        # API success with `origin="pypi"`, but no `pip_freeze`
        res = await client.post(
            f"api/v2/task-group/{task_pypi.taskgroupv2_id}/reactivate/"
        )
        assert res.status_code == 422
        assert "task_group.pip_freeze=None" in res.json()["detail"]

        # Set pip_freeze
        task_group_pypi = await db.get(TaskGroupV2, task_pypi.taskgroupv2_id)
        task_group_pypi.pip_freeze = "devtools==0.12.0"
        db.add(task_group_pypi)
        await db.commit()
        await db.refresh(task_group_pypi)

        # API success with `origin="pypi"`
        res = await client.post(
            f"api/v2/task-group/{task_group_pypi.id}/reactivate/"
        )
        activity = res.json()
        activity_id = activity["id"]
        assert res.status_code == 202
        assert activity["version"] == task_group_pypi.version
        assert activity["status"] == TaskGroupActivityStatusV2.PENDING
        assert activity["action"] == TaskGroupActivityActionV2.REACTIVATE
        assert activity["timestamp_started"] is not None
        assert activity["timestamp_ended"] is None
        await db.refresh(task_group_pypi)

        # Check that background task failed
        res = await client.get(f"api/v2/task-group/activity/{activity_id}/")
        assert res.json()["status"] == "failed"


@pytest.mark.parametrize("FRACTAL_RUNNER_BACKEND", ["local", "slurm_ssh"])
async def test_lifecycle(
    client,
    MockCurrentUser,
    db,
    testdata_path,
    FRACTAL_RUNNER_BACKEND,
    override_settings_factory,
    app,
    tmp777_path: Path,
    request,
    current_py_version,
):
    overrides = dict(FRACTAL_RUNNER_BACKEND=FRACTAL_RUNNER_BACKEND)
    if FRACTAL_RUNNER_BACKEND == "slurm_ssh":
        # Setup remote Python interpreter
        current_py_version_underscore = current_py_version.replace(".", "_")
        python_key = f"FRACTAL_TASKS_PYTHON_{current_py_version_underscore}"
        python_value = (
            f"/.venv{current_py_version}/bin/python{current_py_version}"
        )
        overrides[python_key] = python_value
    override_settings_factory(**overrides)

    if FRACTAL_RUNNER_BACKEND == "slurm_ssh":
        app.state.fractal_ssh_list = request.getfixturevalue(
            "fractal_ssh_list"
        )
        slurmlogin_ip = request.getfixturevalue("slurmlogin_ip")
        ssh_keys = request.getfixturevalue("ssh_keys")
        user_settings_dict = dict(
            ssh_host=slurmlogin_ip,
            ssh_username=SLURM_USER,
            ssh_private_key_path=ssh_keys["private"],
            ssh_tasks_dir=(tmp777_path / "tasks").as_posix(),
            ssh_jobs_dir=(tmp777_path / "artifacts").as_posix(),
        )
    else:
        user_settings_dict = {}

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
        files = {"file": (wheel_path.name, f, "application/zip")}
        async with MockCurrentUser(
            user_kwargs=dict(is_verified=True),
            user_settings_dict=user_settings_dict,
        ):
            # STEP 1: Task collection
            res = await client.post(
                "api/v2/task/collect/pip/",
                data=dict(package=wheel_path.as_posix()),
                files=files,
            )
            assert res.status_code == 202
            activity = res.json()
            activity_id = activity["id"]
            task_group_id = activity["taskgroupv2_id"]
            res = await client.get(
                f"/api/v2/task-group/activity/{activity_id}/"
            )
            assert res.status_code == 200
            task_group_activity = res.json()
            assert task_group_activity["status"] == "OK"

            # STEP 2: Deactivate task group
            res = await client.post(
                f"api/v2/task-group/{task_group_id}/deactivate/"
            )
            assert res.status_code == 202
            activity_id = res.json()["id"]
            res = await client.get(
                f"api/v2/task-group/activity/{activity_id}/"
            )
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
            res = await client.get(
                f"api/v2/task-group/activity/{activity_id}/"
            )
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
            # which has no pip-freeze informationre 2.9.0, which has no
            task_group.pip_freeze = None
            db.add(task_group)
            await db.commit()
            await db.refresh(task_group)
            res = await client.post(
                f"api/v2/task-group/{task_group_id}/deactivate/"
            )
            assert res.status_code == 202
            activity_id = res.json()["id"]
            res = await client.get(
                f"api/v2/task-group/activity/{activity_id}/"
            )
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


async def test_fail_due_to_ongoing_activities(
    client,
    MockCurrentUser,
    db,
    task_factory_v2,
):
    """
    Test that deactivate/reactivate endpoints fail if other
    activities for the same task group are ongoing.
    """

    async with MockCurrentUser() as user:
        # Create mock objects
        task = await task_factory_v2(user_id=user.id, name="task")
        task_group = await db.get(TaskGroupV2, task.taskgroupv2_id)
        db.add(task_group)
        await db.commit()
        await db.refresh(task_group)
        activity = TaskGroupActivityV2(
            user_id=user.id,
            taskgroupv2_id=task_group.id,
            action=TaskGroupActivityActionV2.DEACTIVATE,
            status=TaskGroupActivityStatusV2.ONGOING,
            pkg_name="dummy",
            version="dummy",
        )
        db.add(activity)
        await db.commit()

        # API failure for deactivate
        res = await client.post(
            f"api/v2/task-group/{task_group.id}/deactivate/"
        )
        assert res.status_code == 422
        assert "Found ongoing activities" in res.json()["detail"]

        # Set active to False
        task_group.active = False
        db.add(task_group)
        await db.commit()
        await db.refresh(task_group)

        # API failure for reactivate
        res = await client.post(
            f"api/v2/task-group/{task_group.id}/reactivate/"
        )
        assert res.status_code == 422
        assert "Found ongoing activities" in res.json()["detail"]


async def test_lifecycle_actions_with_submitted_jobs(
    db,
    client,
    MockCurrentUser,
    task_factory_v2,
    project_factory_v2,
    workflow_factory_v2,
    dataset_factory_v2,
):
    async with MockCurrentUser() as user:
        # Create mock task groups
        active_task = await task_factory_v2(
            user_id=user.id,
            name="task-active",
            task_group_kwargs=dict(active=True),
        )
        non_active_task = await task_factory_v2(
            user_id=user.id,
            name="task-non-active",
            task_group_kwargs=dict(active=False),
        )
        p = await project_factory_v2(user=user)
        wf = await workflow_factory_v2()
        ds = await dataset_factory_v2()
        for task in [active_task, non_active_task]:
            await _workflow_insert_task(
                workflow_id=wf.id,
                task_id=task.id,
                db=db,
            )
        db.add(
            JobV2(
                project_id=p.id,
                workflow_id=wf.id,
                dataset_id=ds.id,
                user_email=user.email,
                dataset_dump={},
                workflow_dump={},
                project_dump={},
                status=JobStatusTypeV2.SUBMITTED,
                first_task_index=0,
                last_task_index=1,
            )
        )
        await db.commit()

        res = await client.post(
            f"api/v2/task-group/{active_task.taskgroupv2_id}/deactivate/"
        )
        assert res.status_code == 422
        assert "submitted jobs use its tasks" in res.json()["detail"]

        res = await client.post(
            f"api/v2/task-group/{non_active_task.taskgroupv2_id}/reactivate/"
        )
        assert res.status_code == 422
        assert "submitted jobs use its tasks" in res.json()["detail"]
