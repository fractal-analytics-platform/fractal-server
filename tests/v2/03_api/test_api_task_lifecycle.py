from devtools import debug  # noqa

from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.schemas.v2 import TaskGroupActivityActionV2
from fractal_server.app.schemas.v2 import TaskGroupActivityStatusV2


async def test_deactivate_task_group_api(
    client,
    MockCurrentUser,
    db,
    task_factory_v2,
):
    """
    This tests _only_ the API of the `deactivate` endpoint.
    """
    async with MockCurrentUser() as different_user:
        non_accessible_task = await task_factory_v2(
            user_id=different_user.id, name="task"
        )
        non_accessible_task_group = await db.get(
            TaskGroupV2, non_accessible_task.taskgroupv2_id
        )

    async with MockCurrentUser() as user:
        # Create mock task groups
        non_active_task = await task_factory_v2(user_id=user.id, name="task")
        task_other = await task_factory_v2(user_id=user.id, name="task")
        task_pypi = await task_factory_v2(
            user_id=user.id, name="task", version="1.2.3"
        )
        non_active_task_group = await db.get(
            TaskGroupV2, non_active_task.taskgroupv2_id
        )
        task_group_other = await db.get(TaskGroupV2, task_other.taskgroupv2_id)
        task_group_pypi = await db.get(TaskGroupV2, task_pypi.taskgroupv2_id)
        non_active_task_group.active = False
        task_group_other.origin = "other"
        task_group_pypi.origin = "pypi"
        db.add(non_active_task_group)
        db.add(task_group_other)
        db.add(task_group_pypi)
        await db.commit()
        await db.refresh(non_active_task_group)
        await db.refresh(task_group_other)
        await db.refresh(task_group_pypi)

        # API failure: Not full access to another user's task group
        res = await client.post(
            f"api/v2/task-group/{non_accessible_task_group.id}/deactivate/"
        )
        assert res.status_code == 403

        # API failure: Non-active task group cannot be deactivated
        res = await client.post(
            f"api/v2/task-group/{non_active_task_group.id}/deactivate/"
        )
        assert res.status_code == 422

        # API success with `origin="other"`
        res = await client.post(
            f"api/v2/task-group/{task_group_other.id}/deactivate/"
        )
        activity = res.json()
        assert res.status_code == 202
        assert activity["version"] == "N/A"
        assert activity["status"] == TaskGroupActivityStatusV2.OK
        assert activity["action"] == TaskGroupActivityActionV2.DEACTIVATE
        assert activity["timestamp_started"] is not None
        assert activity["timestamp_ended"] is not None
        await db.refresh(task_group_other)
        assert task_group_other.active is False

        # API success with `origin="pypi"`
        res = await client.post(
            f"api/v2/task-group/{task_group_pypi.id}/deactivate/"
        )
        activity = res.json()
        assert res.status_code == 202
        debug(activity)
        assert activity["version"] == task_group_pypi.version
        assert activity["status"] == TaskGroupActivityStatusV2.PENDING
        assert activity["action"] == TaskGroupActivityActionV2.DEACTIVATE
        assert activity["timestamp_started"] is not None
        assert activity["timestamp_ended"] is None
        await db.refresh(task_group_pypi)
        assert task_group_pypi.active is False


async def test_reactivate_task_group_api(
    client,
    MockCurrentUser,
    db,
    task_factory_v2,
):
    """
    This tests _only_ the API of the `reactivate` endpoint.
    """
    async with MockCurrentUser() as different_user:
        non_accessible_task = await task_factory_v2(
            user_id=different_user.id, name="task"
        )
        non_accessible_task_group = await db.get(
            TaskGroupV2, non_accessible_task.taskgroupv2_id
        )

    async with MockCurrentUser() as user:
        # Create mock task groups
        active_task = await task_factory_v2(user_id=user.id, name="task")
        task_other = await task_factory_v2(user_id=user.id, name="task")
        task_pypi = await task_factory_v2(
            user_id=user.id, name="task", version="1.2.3"
        )
        active_task_group = await db.get(
            TaskGroupV2, active_task.taskgroupv2_id
        )
        task_group_other = await db.get(TaskGroupV2, task_other.taskgroupv2_id)
        task_group_pypi = await db.get(TaskGroupV2, task_pypi.taskgroupv2_id)
        active_task_group.active = True
        task_group_other.origin = "other"
        task_group_other.active = False
        task_group_pypi.origin = "pypi"
        task_group_pypi.active = False
        db.add(active_task_group)
        db.add(task_group_other)
        db.add(task_group_pypi)
        await db.commit()
        await db.refresh(active_task_group)
        await db.refresh(task_group_other)
        await db.refresh(task_group_pypi)

        # API failure: Not full access to another user's task group
        res = await client.post(
            f"api/v2/task-group/{non_accessible_task_group.id}/reactivate/"
        )
        assert res.status_code == 403

        # API failure: Active task group cannot be reactivated
        res = await client.post(
            f"api/v2/task-group/{active_task_group.id}/reactivate/"
        )
        assert res.status_code == 422

        # API success with `origin="other"`
        res = await client.post(
            f"api/v2/task-group/{task_group_other.id}/reactivate/"
        )
        activity = res.json()
        assert res.status_code == 202
        assert activity["version"] == "N/A"
        assert activity["status"] == TaskGroupActivityStatusV2.OK
        assert activity["action"] == TaskGroupActivityActionV2.REACTIVATE
        assert activity["timestamp_started"] is not None
        assert activity["timestamp_ended"] is not None
        await db.refresh(task_group_other)
        assert task_group_other.active is True

        # API success with `origin="pypi"`
        res = await client.post(
            f"api/v2/task-group/{task_group_pypi.id}/reactivate/"
        )
        activity = res.json()
        assert res.status_code == 202
        debug(activity)
        assert activity["version"] == task_group_pypi.version
        assert activity["status"] == TaskGroupActivityStatusV2.PENDING
        assert activity["action"] == TaskGroupActivityActionV2.REACTIVATE
        assert activity["timestamp_started"] is not None
        assert activity["timestamp_ended"] is None
