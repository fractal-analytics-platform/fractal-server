import shutil
from pathlib import Path

import pytest
from devtools import debug

import fractal_server.app.routes.api.v2.task_collection as task_collection
from fractal_server.app.models.v2 import JobV2
from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.schemas.v2 import JobStatusType
from fractal_server.app.schemas.v2 import ResourceType
from fractal_server.app.schemas.v2 import TaskGroupActivityAction
from fractal_server.app.schemas.v2 import TaskGroupActivityStatus
from fractal_server.config import get_settings
from fractal_server.syringe import Inject

settings = Inject(get_settings)


class MockFractalSSHList:
    """
    Implement the only method which is used from within the API.
    """

    def get(self, *args, **kwargs):
        return None


@pytest.mark.parametrize(
    "FRACTAL_RUNNER_BACKEND", [ResourceType.LOCAL, ResourceType.SLURM_SSH]
)
async def test_deactivate_task_group_api(
    app,
    client,
    MockCurrentUser,
    db,
    task_factory,
    FRACTAL_RUNNER_BACKEND,
    slurm_ssh_resource_profile_fake_db,
    local_resource_profile_db,
):
    """
    This tests _only_ the API of the `deactivate` endpoint.
    """

    async with MockCurrentUser() as different_user:
        non_accessible_task = await task_factory(
            user_id=different_user.id, name="task"
        )

    if FRACTAL_RUNNER_BACKEND == ResourceType.SLURM_SSH:
        app.state.fractal_ssh_list = MockFractalSSHList()
        resource, profile = slurm_ssh_resource_profile_fake_db
    else:
        resource, profile = local_resource_profile_db

    async with MockCurrentUser(user_kwargs=dict(profile_id=profile.id)) as user:
        # Create mock task groups
        non_active_task = await task_factory(
            user_id=user.id,
            name="task1",
            task_group_kwargs=dict(active=False),
        )
        task_other = await task_factory(
            user_id=user.id,
            version=None,
            name="task2",
            task_group_kwargs=dict(origin="other"),
        )
        task_pypi = await task_factory(
            user_id=user.id,
            name="task3",
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
        assert activity["status"] == TaskGroupActivityStatus.OK
        assert activity["action"] == TaskGroupActivityAction.DEACTIVATE
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
        assert activity["status"] == TaskGroupActivityStatus.PENDING
        assert activity["action"] == TaskGroupActivityAction.DEACTIVATE
        assert activity["timestamp_started"] is not None
        assert activity["timestamp_ended"] is None
        task_group_pypi = await db.get(TaskGroupV2, task_pypi.taskgroupv2_id)
        assert activity["version"] == task_group_pypi.version
        assert task_group_pypi.active is False

        # Check that background task failed
        res = await client.get(f"api/v2/task-group/activity/{activity_id}/")
        assert res.json()["status"] == "failed"
        if FRACTAL_RUNNER_BACKEND == ResourceType.SLURM_SSH:
            assert "Cannot establish SSH connection" in res.json()["log"]
        else:
            assert "does not exist" in res.json()["log"]


@pytest.mark.parametrize(
    "FRACTAL_RUNNER_BACKEND", [ResourceType.LOCAL, ResourceType.SLURM_SSH]
)
async def test_reactivate_task_group_api(
    app,
    client,
    MockCurrentUser,
    db,
    task_factory,
    current_py_version,
    FRACTAL_RUNNER_BACKEND,
    slurm_ssh_resource_profile_fake_db,
    local_resource_profile_db,
):
    """
    This tests _only_ the API of the `reactivate` endpoint.
    """

    async with MockCurrentUser() as different_user:
        non_accessible_task = await task_factory(
            user_id=different_user.id, name="task1"
        )

    if FRACTAL_RUNNER_BACKEND == ResourceType.SLURM_SSH:
        resource, profile = slurm_ssh_resource_profile_fake_db
        app.state.fractal_ssh_list = MockFractalSSHList()
    else:
        resource, profile = local_resource_profile_db
    async with MockCurrentUser(user_kwargs=dict(profile_id=profile.id)) as user:
        # Create mock task groups
        active_task = await task_factory(user_id=user.id, name="task2")
        task_other = await task_factory(
            user_id=user.id,
            version=None,
            name="task3",
            task_group_kwargs=dict(active=False),
        )
        task_pypi = await task_factory(
            user_id=user.id,
            name="task4",
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
        assert activity["status"] == TaskGroupActivityStatus.OK
        assert activity["action"] == TaskGroupActivityAction.REACTIVATE
        assert activity["timestamp_started"] is not None
        assert activity["timestamp_ended"] is not None
        task_group_other = await db.get(TaskGroupV2, task_other.taskgroupv2_id)
        assert task_group_other.active is True

        # API success with `origin="pypi"`, but no `env_info`
        res = await client.post(
            f"api/v2/task-group/{task_pypi.taskgroupv2_id}/reactivate/"
        )
        assert res.status_code == 422
        assert "task_group.env_info=None" in res.json()["detail"]

        # Set env_info
        task_group_pypi = await db.get(TaskGroupV2, task_pypi.taskgroupv2_id)
        task_group_pypi.env_info = "devtools==0.12.0"
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
        assert activity["status"] == TaskGroupActivityStatus.PENDING
        assert activity["action"] == TaskGroupActivityAction.REACTIVATE
        assert activity["timestamp_started"] is not None
        assert activity["timestamp_ended"] is None
        await db.refresh(task_group_pypi)

        # Check that background task failed
        res = await client.get(f"api/v2/task-group/activity/{activity_id}/")
        assert res.json()["status"] == "failed"


async def _aux_test_lifecycle(
    *,
    client,
    MockCurrentUser,
    db,
    testdata_path,
    tmp777_path: Path,
    monkeypatch,
    profile,
):
    # Absolute path to wheel file (use a path in tmp77_path, so that it is
    # also accessible on the SSH remote host)
    old_archive_path = (
        testdata_path.parent
        / "v2/fractal_tasks_mock/dist"
        / "fractal_tasks_mock-0.0.1-py3-none-any.whl"
    )
    archive_path = tmp777_path / old_archive_path.name
    shutil.copy(old_archive_path, archive_path)
    with open(archive_path, "rb") as f:
        files = {"file": (archive_path.name, f.read(), "application/zip")}

    async with MockCurrentUser(
        user_kwargs=dict(
            is_verified=True,
            profile_id=profile.id,
        )
    ) as user:
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
        task_group_activity_collection = res.json()
        assert task_group_activity_collection["status"] == "OK"
        assert task_group_activity_collection["timestamp_ended"] is not None

        log = task_group_activity_collection["log"]
        assert log is not None
        assert log.count("\n") > 0
        assert log.count("\\n") == 0

        task_groupv2_id = task_group_activity_collection["taskgroupv2_id"]
        # Check env_info attribute in TaskGroupV2
        db.expunge_all()
        task_group = await db.get(TaskGroupV2, task_groupv2_id)
        env_info = task_group.env_info
        task_group_archive_path = task_group.archive_path
        assert (
            f"fractal-tasks-mock @ file://{task_group_archive_path}" in env_info
        )
        assert (Path(task_group.path) / Path(archive_path).name).as_posix() == (
            Path(task_group_archive_path).as_posix()
        )

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
        db.expunge_all()
        task_group = await db.get(TaskGroupV2, task_group_id)
        assert task_group.active is False
        assert Path(task_group.path).exists()
        assert not Path(task_group.venv_path).exists()
        assert Path(task_group.archive_path).exists()

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
        assert Path(task_group.archive_path).exists()

        # STEP 4: Deactivate a task group created before 2.9.0,
        # which has no pip-freeze information
        task_group.env_info = None
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
        assert task_group.env_info is not None
        assert Path(task_group.path).exists()
        assert not Path(task_group.venv_path).exists()
        assert Path(task_group.archive_path).exists()

        # STEP 5: Delete task group
        # Assert that we must DELETE the task group before collect again

        # Collection fails
        res = await client.post("api/v2/task/collect/pip/", files=files)
        assert res.status_code == 422
        assert res.json()["detail"] == (
            f"User '{user.email}' already owns a task group with "
            f"name='{task_group.pkg_name}' "
            f"and version='{task_group.version}'.\n"
            "Note: There exists another task-group collection "
            f"(activity ID={task_group_activity_collection['id']}) "
            "for this task group "
            f"(ID={task_group_activity_collection['taskgroupv2_id']}), "
            f"with status '{TaskGroupActivityStatus.OK}'."
        )

        task_group_path = Path(task_group.path)
        assert task_group_path.exists()
        # We delete the task group
        res = await client.post(f"api/v2/task-group/{task_group_id}/delete/")
        debug(res.json())
        assert res.status_code == 202
        activity = res.json()
        assert activity["action"] == TaskGroupActivityAction.DELETE
        assert activity["status"] == TaskGroupActivityStatus.PENDING
        # `task_group.path` does not exist anymore
        assert not Path(task_group.path).exists()

        res = await client.get(f"api/v2/task-group/activity/{activity['id']}/")
        activity = res.json()
        assert activity["action"] == TaskGroupActivityAction.DELETE
        assert activity["status"] == TaskGroupActivityStatus.OK

        # We call the collect endpoint again, mocking the backgroud tasks
        # (for speeding up the test)
        def dummy_collect(*args, **kwargs):
            pass

        monkeypatch.setattr(task_collection, "collect_local", dummy_collect)
        monkeypatch.setattr(task_collection, "collect_ssh", dummy_collect)

        res = await client.post("api/v2/task/collect/pip/", files=files)
        assert res.status_code == 202

        task_group_id = res.json()["taskgroupv2_id"]
        task_group = await db.get(TaskGroupV2, task_group_id)
        task_group_path = Path(task_group.path)

        # New deletion must fail (because the collection was mocked)
        res = await client.post(f"api/v2/task-group/{task_group_id}/delete/")
        assert res.status_code == 202
        activity = res.json()
        assert activity["action"] == TaskGroupActivityAction.DELETE
        assert activity["status"] == TaskGroupActivityStatus.PENDING
        res = await client.get(f"api/v2/task-group/activity/{activity['id']}/")
        activity = res.json()
        assert activity["action"] == TaskGroupActivityAction.DELETE
        assert activity["status"] == TaskGroupActivityStatus.FAILED
        assert "No such file or directory" in activity["log"]


async def test_lifecycle_local(
    client,
    MockCurrentUser,
    db,
    testdata_path,
    override_settings_factory,
    tmp777_path: Path,
    monkeypatch,
    local_resource_profile_db,
):
    override_settings_factory(FRACTAL_RUNNER_BACKEND="local")
    resource, profile = local_resource_profile_db

    await _aux_test_lifecycle(
        client=client,
        MockCurrentUser=MockCurrentUser,
        db=db,
        testdata_path=testdata_path,
        tmp777_path=tmp777_path,
        monkeypatch=monkeypatch,
        profile=profile,
    )


@pytest.mark.container
@pytest.mark.ssh
async def test_lifecycle_slurm_ssh(
    client,
    MockCurrentUser,
    db,
    testdata_path,
    override_settings_factory,
    tmp777_path: Path,
    monkeypatch,
    slurm_ssh_resource_profile_db,
    fractal_ssh_list,
    app,
):
    override_settings_factory(FRACTAL_RUNNER_BACKEND="slurm_ssh")

    app.state.fractal_ssh_list = fractal_ssh_list
    resource, profile = slurm_ssh_resource_profile_db

    await _aux_test_lifecycle(
        client=client,
        MockCurrentUser=MockCurrentUser,
        db=db,
        testdata_path=testdata_path,
        tmp777_path=tmp777_path,
        monkeypatch=monkeypatch,
        profile=profile,
    )


async def test_fail_due_to_ongoing_activities(
    client, MockCurrentUser, db, task_factory, local_resource_profile_db
):
    """
    Test that deactivate/reactivate endpoints fail if other
    activities for the same task group are ongoing.
    """
    resource, profile = local_resource_profile_db
    async with MockCurrentUser(user_kwargs=dict(profile_id=profile.id)) as user:
        # Create mock objects
        task = await task_factory(user_id=user.id, name="task")
        task_group = await db.get(TaskGroupV2, task.taskgroupv2_id)
        db.add(task_group)
        await db.commit()
        await db.refresh(task_group)
        activity = TaskGroupActivityV2(
            user_id=user.id,
            taskgroupv2_id=task_group.id,
            action=TaskGroupActivityAction.DEACTIVATE,
            status=TaskGroupActivityStatus.ONGOING,
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
    task_factory,
    project_factory,
    workflow_factory,
    dataset_factory,
    local_resource_profile_db,
):
    resource, profile = local_resource_profile_db
    async with MockCurrentUser(user_kwargs=dict(profile_id=profile.id)) as user:
        # Create mock task groups
        active_task = await task_factory(
            user_id=user.id,
            name="task-active",
            task_group_kwargs=dict(active=True),
        )
        non_active_task = await task_factory(
            user_id=user.id,
            name="task-non-active",
            task_group_kwargs=dict(active=False),
        )
        p = await project_factory(user=user)
        wf = await workflow_factory()
        ds = await dataset_factory()
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
                status=JobStatusType.SUBMITTED,
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
