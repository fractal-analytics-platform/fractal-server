from urllib.parse import quote

import pytest
from devtools import debug

from fractal_server.app.models import TaskGroupV2
from fractal_server.app.models import UserGroup
from fractal_server.app.models.v2 import JobV2
from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.schemas.v2 import JobStatusTypeV2
from fractal_server.app.schemas.v2 import ResourceType
from fractal_server.app.schemas.v2 import TaskGroupActivityActionV2
from fractal_server.app.schemas.v2 import TaskGroupActivityStatusV2

PREFIX = "/admin/v2"


async def test_task_group_admin(
    db,
    client,
    MockCurrentUser,
    project_factory_v2,
    workflow_factory_v2,
    workflowtask_factory_v2,
    task_factory_v2,
):
    async with MockCurrentUser() as user1:
        task1 = await task_factory_v2(
            user_id=user1.id,
            name="AaAa",
        )
        res = await client.get(f"/api/v2/task-group/{task1.taskgroupv2_id}/")
        task_group_1 = res.json()

        assert "resource_id" not in task_group_1

        task2 = await task_factory_v2(
            name="BBB",
            user_id=user1.id,
            task_group_kwargs=dict(active=False),
        )
        # make task_group_2 private
        task_group_2 = await db.get(TaskGroupV2, task2.taskgroupv2_id)
        task_group_2.user_group_id = None
        db.add(task_group_2)
        await db.commit()
        res = await client.get(f"/api/v2/task-group/{task2.taskgroupv2_id}/")
        task_group_2 = res.json()
        assert "resource_id" not in task_group_2
        debug(task_group_2)

    async with MockCurrentUser() as user2:
        task3 = await task_factory_v2(user_id=user2.id, name="bbbbbbbb")
        res = await client.get(f"/api/v2/task-group/{task3.taskgroupv2_id}/")
        task_group_3 = res.json()
        assert "resource_id" not in task_group_3

    async with MockCurrentUser(user_kwargs={"is_superuser": True}):
        # GET /{id}/
        for task_group in [task_group_1, task_group_2, task_group_3]:
            res = await client.get(f"{PREFIX}/task-group/{task_group['id']}/")
            assert res.status_code == 200
            assert "resource_id" in res.json()

        res = await client.get(f"{PREFIX}/task-group/9999/")
        assert res.status_code == 404

        # GET /
        res = await client.get(f"{PREFIX}/task-group/?page_size=1000")
        assert res.status_code == 200
        assert len(res.json()["items"]) == 3
        groups = sorted(
            res.json()["items"], key=lambda x: x["timestamp_last_used"]
        )

        # Filter using `user_id`
        res = await client.get(f"{PREFIX}/task-group/?user_id={user1.id}")
        assert res.status_code == 200
        assert len(res.json()["items"]) == 2
        res = await client.get(f"{PREFIX}/task-group/?user_id={user2.id}")
        assert res.status_code == 200
        assert len(res.json()["items"]) == 1

        # Filter using `timestamp_last_used_min`
        res = await client.get(
            f"{PREFIX}/task-group/?timestamp_last_used_min="
            f"{quote(groups[1]['timestamp_last_used'])}"
        )
        debug(res.json())
        assert res.status_code == 200
        assert len(res.json()["items"]) == 2
        res = await client.get(
            f"{PREFIX}/task-group/?timestamp_last_used_min="
            f"{quote(groups[0]['timestamp_last_used'])}"
        )
        assert res.status_code == 200
        assert len(res.json()["items"]) == 3

        # Filter using `timestamp_last_used_max`
        res = await client.get(
            f"{PREFIX}/task-group/?timestamp_last_used_max="
            f"{quote(groups[1]['timestamp_last_used'])}"
        )
        assert res.status_code == 200
        assert len(res.json()["items"]) == 2
        res = await client.get(
            f"{PREFIX}/task-group/?timestamp_last_used_max="
            f"{quote(groups[0]['timestamp_last_used'])}"
        )
        assert res.status_code == 200
        assert len(res.json()["items"]) == 1

        # Filter using `origin`
        res = await client.get(f"{PREFIX}/task-group/?origin=other")
        assert res.status_code == 200
        assert len(res.json()["items"]) == 3
        res = await client.get(f"{PREFIX}/task-group/?origin=pypi")
        assert res.status_code == 200
        assert len(res.json()["items"]) == 0
        res = await client.get(f"{PREFIX}/task-group/?origin=INVALID")
        assert res.status_code == 422

        # Filter using `pkg_name`
        res = await client.get(f"{PREFIX}/task-group/?pkg_name=bb")
        assert res.status_code == 200
        assert len(res.json()["items"]) == 2

        # Filter using `active`
        res = await client.get(f"{PREFIX}/task-group/?active=true")
        assert res.status_code == 200
        assert len(res.json()["items"]) == 2
        res = await client.get(
            f"{PREFIX}/task-group/?user_id={user1.id}&active=true"
        )
        assert res.status_code == 200
        assert len(res.json()["items"]) == 1
        res = await client.get(
            f"{PREFIX}/task-group/?user_id={user1.id}&active=false"
        )
        assert res.status_code == 200
        assert len(res.json()["items"]) == 1

        # Filter using `private`
        res = await client.get(f"{PREFIX}/task-group/?private=true")
        assert res.status_code == 200
        assert len(res.json()["items"]) == 1
        res = await client.get(f"{PREFIX}/task-group/?private=false")
        assert res.status_code == 200
        assert len(res.json()["items"]) == 2

        # Filter using `resource_id` (assuming they all have the same resource)
        resource_id = res.json()["items"][0]["resource_id"]
        res = await client.get(
            f"{PREFIX}/task-group/?resource_id={resource_id}"
        )
        assert res.status_code == 200
        assert len(res.json()["items"]) == 3

        # Filter using `user_group_id` and/or `private`
        res = await client.get(f"{PREFIX}/task-group/?user_group_id=1")
        assert res.status_code == 200
        assert len(res.json()["items"]) == 2
        res = await client.get(
            f"{PREFIX}/task-group/?user_group_id=1&private=true"
        )
        assert res.status_code == 422

        # PATCH /{id}/
        res = await client.patch(
            f"{PREFIX}/task-group/9999/", json=dict(user_group_id=None)
        )
        assert res.status_code == 404

        user_group = UserGroup(name="foo")
        db.add(user_group)
        await db.commit()
        await db.refresh(user_group)
        res = await client.patch(
            f"{PREFIX}/task-group/{task_group_1['id']}/",
            json=dict(user_group_id=user_group.id),
        )
        assert res.status_code == 403
        res = await client.patch(
            f"{PREFIX}/task-group/{task_group_1['id']}/",
            json=dict(user_group_id=None),
        )
        assert res.status_code == 200
        res = await client.get(f"{PREFIX}/task-group/?private=true")
        assert len(res.json()["items"]) == 2
        res = await client.get(f"{PREFIX}/task-group/?active=true")
        assert len(res.json()["items"]) == 2


async def test_get_task_group_activity(
    client, MockCurrentUser, db, task_factory_v2
):
    async with MockCurrentUser() as user1:
        activity1 = TaskGroupActivityV2(
            user_id=user1.id,
            pkg_name="foo",
            version="1",
            status=TaskGroupActivityStatusV2.OK,
            action=TaskGroupActivityActionV2.COLLECT,
        )
        activity2 = TaskGroupActivityV2(
            user_id=user1.id,
            pkg_name="bar",
            version="1",
            status=TaskGroupActivityStatusV2.OK,
            action=TaskGroupActivityActionV2.REACTIVATE,
        )
    async with MockCurrentUser() as user2:
        task = await task_factory_v2(user_id=user2.id)
        activity3 = TaskGroupActivityV2(
            user_id=user2.id,
            pkg_name="foo",
            version="2",
            status=TaskGroupActivityStatusV2.FAILED,
            action=TaskGroupActivityActionV2.COLLECT,
            taskgroupv2_id=task.taskgroupv2_id,
        )
        activity4 = TaskGroupActivityV2(
            user_id=user2.id,
            pkg_name="foo",
            version="1",
            status=TaskGroupActivityStatusV2.OK,
            action=TaskGroupActivityActionV2.COLLECT,
            taskgroupv2_id=task.taskgroupv2_id,
        )

    for activity in [activity1, activity2, activity3, activity4]:
        db.add(activity)
    await db.commit()
    for activity in [activity1, activity2, activity3, activity4]:
        await db.refresh(activity)

    async with MockCurrentUser():
        res = await client.get(f"{PREFIX}/task-group/activity/")
        assert res.status_code == 401

    async with MockCurrentUser(user_kwargs={"is_superuser": True}):
        res = await client.get(f"{PREFIX}/task-group/activity/?page_size=1000")
        assert res.status_code == 200
        assert len(res.json()) == 4

        # user_id
        res = await client.get(
            f"{PREFIX}/task-group/activity/?user_id={user1.id}"
        )
        assert len(res.json()["items"]) == 2
        res = await client.get(
            f"{PREFIX}/task-group/activity/?user_id={user2.id}"
        )
        assert len(res.json()["items"]) == 2
        # task_group_activity_id
        res = await client.get(
            f"{PREFIX}/task-group/activity/"
            f"?user_id={user1.id}&task_group_activity_id={activity1.id}"
        )
        assert len(res.json()["items"]) == 1
        # taskgroupv2_id
        res = await client.get(
            f"{PREFIX}/task-group/activity/"
            f"?taskgroupv2_id={task.taskgroupv2_id}"
        )
        assert len(res.json()["items"]) == 2
        # pkg_name
        res = await client.get(f"{PREFIX}/task-group/activity/?pkg_name=foo")
        assert len(res.json()["items"]) == 3
        res = await client.get(f"{PREFIX}/task-group/activity/?pkg_name=bar")
        assert len(res.json()["items"]) == 1
        res = await client.get(f"{PREFIX}/task-group/activity/?pkg_name=xxx")
        assert len(res.json()["items"]) == 0
        # status
        res = await client.get(f"{PREFIX}/task-group/activity/?status=OK")
        assert len(res.json()["items"]) == 3
        res = await client.get(f"{PREFIX}/task-group/activity/?status=failed")
        assert len(res.json()["items"]) == 1
        res = await client.get(f"{PREFIX}/task-group/activity/?status=ongoing")
        assert len(res.json()["items"]) == 0
        res = await client.get(f"{PREFIX}/task-group/activity/?status=xxx")
        assert res.status_code == 422
        # action
        res = await client.get(f"{PREFIX}/task-group/activity/?action=collect")
        assert len(res.json()["items"]) == 3
        res = await client.get(
            f"{PREFIX}/task-group/activity/?action=reactivate"
        )
        assert len(res.json()["items"]) == 1
        res = await client.get(
            f"{PREFIX}/task-group/activity/?action=deactivate"
        )
        assert len(res.json()["items"]) == 0
        res = await client.get(f"{PREFIX}/task-group/activity/?action=xxx")
        assert res.status_code == 422
        # timestamp_started_min
        res = await client.get(
            f"{PREFIX}/task-group/activity/"
            f"?timestamp_started_min={quote(str(activity2.timestamp_started))}"
        )
        assert len(res.json()["items"]) == 3
        res = await client.get(
            f"{PREFIX}/task-group/activity/"
            f"?timestamp_started_min={quote(str(activity3.timestamp_started))}"
        )
        assert len(res.json()["items"]) == 2
        # combination and iconstains
        res = await client.get(
            f"{PREFIX}/task-group/activity/?status=OK&pkg_name=O"
        )
        assert len(res.json()["items"]) == 2


class MockFractalSSHList:
    """
    Implement the only method which is used from within the API.
    """

    def get(self, *args, **kwargs):
        return None


@pytest.mark.parametrize(
    "FRACTAL_RUNNER_BACKEND", [ResourceType.LOCAL, ResourceType.SLURM_SSH]
)
async def test_admin_deactivate_task_group_api(
    app,
    client,
    MockCurrentUser,
    db,
    task_factory_v2,
    FRACTAL_RUNNER_BACKEND,
    override_settings_factory,
    local_resource_profile_db,
    slurm_ssh_resource_profile_fake_db,
):
    """
    This tests _only_ the API of the admin's `deactivate` endpoint.
    """
    override_settings_factory(
        FRACTAL_RUNNER_BACKEND=FRACTAL_RUNNER_BACKEND,
    )

    if FRACTAL_RUNNER_BACKEND == ResourceType.SLURM_SSH:
        resource, profile = slurm_ssh_resource_profile_fake_db[:]
        app.state.fractal_ssh_list = MockFractalSSHList()
    else:
        resource, profile = local_resource_profile_db

    async with MockCurrentUser(
        user_kwargs=dict(profile_id=profile.id),
    ) as user:
        # Create mock task groups
        non_active_task = await task_factory_v2(
            user_id=user.id, name="task", task_group_kwargs=dict(active=False)
        )
        task_other = await task_factory_v2(
            user_id=user.id,
            version=None,
            name="task",
        )
        task_pypi = await task_factory_v2(
            user_id=user.id,
            name="task",
            version="1.2.3",
            task_group_kwargs=dict(
                origin="pypi", venv_path="/invalid/so/it/fails"
            ),
        )

    async with MockCurrentUser(
        user_kwargs={"is_superuser": True},
    ):
        # API failure: Non-active task group cannot be deactivated
        res = await client.post(
            f"{PREFIX}/task-group/{non_active_task.taskgroupv2_id}/deactivate/"
        )
        assert res.status_code == 422

        # API success with `origin="other"`
        res = await client.post(
            f"{PREFIX}/task-group/{task_other.taskgroupv2_id}/deactivate/"
        )
        assert res.status_code == 202
        activity = res.json()
        task_group_other = await db.get(TaskGroupV2, task_other.taskgroupv2_id)
        assert activity["version"] == "N/A"
        assert activity["status"] == TaskGroupActivityStatusV2.OK
        assert activity["action"] == TaskGroupActivityActionV2.DEACTIVATE
        assert activity["timestamp_started"] is not None
        assert activity["timestamp_ended"] is not None
        assert task_group_other.active is False

        # API success with `origin="pypi"`
        debug(task_pypi)
        debug(f"{PREFIX}/task-group/{task_pypi.taskgroupv2_id}/deactivate/")
        res = await client.post(
            f"{PREFIX}/task-group/{task_pypi.taskgroupv2_id}/deactivate/"
        )
        debug(res.json())
        assert res.status_code == 202
        activity = res.json()
        task_group_pypi = await db.get(TaskGroupV2, task_pypi.taskgroupv2_id)
        activity_id = activity["id"]
        assert activity["version"] == task_group_pypi.version
        assert activity["status"] == TaskGroupActivityStatusV2.PENDING
        assert activity["action"] == TaskGroupActivityActionV2.DEACTIVATE
        assert activity["timestamp_started"] is not None
        assert activity["timestamp_ended"] is None

        # Background deactivation failed early
        assert task_group_pypi.active is True

        # Check that background task failed
        res = await db.get(TaskGroupActivityV2, activity_id)
        assert res.status == "failed"
        if FRACTAL_RUNNER_BACKEND == ResourceType.SLURM_SSH:
            assert "Cannot establish SSH connection" in res.log
        else:
            assert "does not exist" in res.log


@pytest.mark.parametrize(
    "FRACTAL_RUNNER_BACKEND", [ResourceType.LOCAL, ResourceType.SLURM_SSH]
)
async def test_reactivate_task_group_api(
    app,
    client,
    MockCurrentUser,
    db,
    task_factory_v2,
    current_py_version,
    FRACTAL_RUNNER_BACKEND,
    override_settings_factory,
    local_resource_profile_db,
    slurm_ssh_resource_profile_fake_db,
):
    """
    This tests _only_ the API of the admin `reactivate` endpoint.
    """

    if FRACTAL_RUNNER_BACKEND == ResourceType.SLURM_SSH:
        resource, profile = slurm_ssh_resource_profile_fake_db
        app.state.fractal_ssh_list = MockFractalSSHList()
    else:
        resource, profile = local_resource_profile_db

    async with MockCurrentUser(user_kwargs=dict(profile_id=profile.id)) as user:
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

    async with MockCurrentUser(
        user_kwargs={"is_superuser": True},
    ):
        # API failure: Active task group cannot be reactivated
        res = await client.post(
            f"{PREFIX}/task-group/{active_task.taskgroupv2_id}/reactivate/"
        )
        assert res.status_code == 422

        # API success with `origin="other"`
        res = await client.post(
            f"{PREFIX}/task-group/{task_other.taskgroupv2_id}/reactivate/"
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

        # API success with `origin="pypi"`, but no `env_info`
        res = await client.post(
            f"{PREFIX}/task-group/{task_pypi.taskgroupv2_id}/reactivate/"
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
            f"{PREFIX}/task-group/{task_group_pypi.id}/reactivate/"
        )
        activity = res.json()
        debug(activity)
        activity_id = activity["id"]
        assert res.status_code == 202
        assert activity["version"] == task_group_pypi.version
        assert activity["status"] == TaskGroupActivityStatusV2.PENDING
        assert activity["action"] == TaskGroupActivityActionV2.REACTIVATE
        assert activity["timestamp_started"] is not None
        assert activity["timestamp_ended"] is None
        await db.refresh(task_group_pypi)

        # Check that background task failed
        activity = await db.get(TaskGroupActivityV2, activity_id)
        assert activity.status == "failed"


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

    async with MockCurrentUser(
        user_kwargs={"is_superuser": True},
    ):
        res = await client.post(
            f"{PREFIX}/task-group/{active_task.taskgroupv2_id}/deactivate/"
        )
        assert res.status_code == 422
        assert "submitted jobs use its tasks" in res.json()["detail"]

        res = await client.post(
            f"{PREFIX}/task-group/{non_active_task.taskgroupv2_id}/reactivate/"
        )
        assert res.status_code == 422
        assert "submitted jobs use its tasks" in res.json()["detail"]


async def test_admin_delete_task_group_api_local(
    client,
    MockCurrentUser,
    task_factory_v2,
    local_resource_profile_db,
):
    resource, profile = local_resource_profile_db

    async with MockCurrentUser(user_kwargs=dict(profile_id=profile.id)) as user:
        task = await task_factory_v2(user_id=user.id, name="task-name")
        res = await client.get(f"/api/v2/task-group/{task.taskgroupv2_id}/")
        task_group_id = res.json()["id"]

    async with MockCurrentUser(user_kwargs={"is_superuser": True}):
        res = await client.get(f"{PREFIX}/task-group/")
        assert len(res.json()["items"]) == 1

        res = await client.post(f"{PREFIX}/task-group/{task_group_id}/delete/")
        assert res.status_code == 202
        activity = res.json()
        activity_id = activity["id"]
        assert activity["action"] == TaskGroupActivityActionV2.DELETE
        assert activity["status"] == TaskGroupActivityStatusV2.PENDING

        res = await client.get(f"{PREFIX}/task-group/activity/?action=delete")
        assert len(res.json()["items"]) == 1
        activity = res.json()["items"][0]
        assert activity["id"] == activity_id
        assert activity["action"] == TaskGroupActivityActionV2.DELETE
        assert activity["status"] == TaskGroupActivityStatusV2.OK


@pytest.mark.container
async def test_admin_delete_task_group_api_ssh(
    client,
    MockCurrentUser,
    app,
    tmp777_path,
    task_factory_v2,
    fractal_ssh_list,
    slurm_ssh_resource_profile_db,
):
    app.state.fractal_ssh_list = fractal_ssh_list
    resource, profile = slurm_ssh_resource_profile_db[:]
    async with MockCurrentUser(user_kwargs=dict(profile_id=profile.id)) as user:
        task = await task_factory_v2(user_id=user.id, name="task-name")
        res = await client.get(f"/api/v2/task-group/{task.taskgroupv2_id}/")
        task_group_id = res.json()["id"]

    async with MockCurrentUser(user_kwargs={"is_superuser": True}):
        res = await client.get(f"{PREFIX}/task-group/")
        assert len(res.json()["items"]) == 1

        res = await client.post(f"{PREFIX}/task-group/{task_group_id}/delete/")
        assert res.status_code == 202
        activity = res.json()
        activity_id = activity["id"]
        assert activity["action"] == TaskGroupActivityActionV2.DELETE
        assert activity["status"] == TaskGroupActivityStatusV2.PENDING

        res = await client.get(f"{PREFIX}/task-group/activity/?action=delete")
        assert len(res.json()["items"]) == 1
        activity = res.json()["items"][0]
        assert activity["id"] == activity_id
        assert activity["action"] == TaskGroupActivityActionV2.DELETE
        assert activity["status"] == TaskGroupActivityStatusV2.OK
