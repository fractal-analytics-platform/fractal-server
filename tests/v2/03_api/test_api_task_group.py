PREFIX = "/api/v2/task-group"


async def test_get_single_task_group(
    client,
    MockCurrentUser,
    task_factory_v2,
):
    async with MockCurrentUser() as user:
        task = await task_factory_v2(user_id=user.id, source="source")
        res = await client.get(f"{PREFIX}/{task.taskgroupv2_id}")
        assert res.json()["user_id"] == user.id
        assert len(res.json()["task_list"]) == 1
        assert res.json()["task_list"][0]["id"] == task.id
