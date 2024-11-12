from fractal_server.app.models.v2 import TaskGroupV2


async def test_deactivate_task_group(
    client,
    MockCurrentUser,
    tmp_path,
    db,
    task_factory_v2,
):
    async with MockCurrentUser() as different_user:
        non_accessible_task = await task_factory_v2(
            user_id=different_user.id, name="task"
        )
        non_accessible_task_group = await db.get(
            TaskGroupV2, non_accessible_task.taskgroupv2_id
        )

    async with MockCurrentUser() as user:
        non_active_task = await task_factory_v2(user_id=user.id, name="task")
        non_active_task_group = await db.get(
            TaskGroupV2, non_active_task.taskgroupv2_id
        )
        non_active_task_group.active = False
        db.add(non_active_task_group)
        await db.commit()
        await db.refresh(non_active_task_group)

        task_other = await task_factory_v2(user_id=user.id, name="task")
        task_group_other = await db.get(TaskGroupV2, task_other.taskgroupv2_id)
        task_group_other.origin = "other"
        db.add(task_group_other)
        await db.commit()
        await db.refresh(task_group_other)

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
