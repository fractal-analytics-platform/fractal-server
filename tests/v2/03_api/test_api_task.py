PREFIX = "/api/v2/task"


async def test_non_verified_user(client, MockCurrentUser):
    """
    Test that a non-verified user is not authorized to make POST/PATCH task
    cals.
    """
    async with MockCurrentUser(user_kwargs=dict(is_verified=False)):
        res = await client.post(f"{PREFIX}/", json={})
        assert res.status_code == 401

        res = await client.patch(f"{PREFIX}/123/", json={})
        assert res.status_code == 401


async def test_task_get_list(db, client, task_factory_v2, MockCurrentUser):
    await task_factory_v2(index=1)
    await task_factory_v2(index=2)
    t = await task_factory_v2(
        index=3,
        args_schema_non_parallel=dict(a=1),
        args_schema_parallel=dict(b=2),
    )

    async with MockCurrentUser():
        res = await client.get(f"{PREFIX}/")
        data = res.json()
        assert res.status_code == 200
        assert len(data) == 3
        assert data[2]["id"] == t.id
        assert data[2]["args_schema_non_parallel"] == dict(a=1)
        assert data[2]["args_schema_parallel"] == dict(b=2)

        res = await client.get(f"{PREFIX}/?args_schema_non_parallel=false")
        assert res.json()[2]["args_schema_non_parallel"] is None
        assert res.json()[2]["args_schema_parallel"] == dict(b=2)

        res = await client.get(f"{PREFIX}/?args_schema_parallel=false")
        assert res.json()[2]["args_schema_non_parallel"] == dict(a=1)
        assert res.json()[2]["args_schema_parallel"] is None

        res = await client.get(
            f"{PREFIX}/"
            "?args_schema_parallel=false&args_schema_non_parallel=False"
        )
        assert res.json()[2]["args_schema_non_parallel"] is None
        assert res.json()[2]["args_schema_parallel"] is None


async def test_get_task(task_factory_v2, client, MockCurrentUser):
    async with MockCurrentUser():
        task = await task_factory_v2(name="name")
        res = await client.get(f"{PREFIX}/{task.id}/")
        assert res.status_code == 200
        res = await client.get(f"{PREFIX}/{task.id+999}/")
        assert res.status_code == 404
        assert res.json()["detail"] == "TaskV2 not found"
