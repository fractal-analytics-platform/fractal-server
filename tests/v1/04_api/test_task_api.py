from devtools import debug


PREFIX = "/api/v1/task"


async def test_task_get_list(db, client, task_factory, MockCurrentUser):
    t0 = await task_factory(name="task0", source="source0")
    t1 = await task_factory(name="task1", source="source1")
    t2 = await task_factory(
        index=2, subtask_list=[t0, t1], args_schema=dict(a=1)
    )

    async with MockCurrentUser():
        res = await client.get(f"{PREFIX}/")
        data = res.json()
        assert res.status_code == 200
        debug(data)
        assert len(data) == 3
        assert data[2]["id"] == t2.id
        assert data[2]["args_schema"] == {"a": 1}

        res = await client.get(f"{PREFIX}/?args_schema=false")
        data = res.json()
        debug(data)
        assert data[2]["args_schema"] is None


async def test_get_task(task_factory, client, MockCurrentUser):
    async with MockCurrentUser():
        task = await task_factory(name="name")
        res = await client.get(f"{PREFIX}/{task.id}/")
        debug(res)
        debug(res.json())
        assert res.status_code == 200
        res = await client.get(f"{PREFIX}/{task.id+999}/")
        assert res.status_code == 404
        assert res.json()["detail"] == "Task not found"
