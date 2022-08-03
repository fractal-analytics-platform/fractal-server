from devtools import debug
from sqlmodel import select

from fractal.server.app.models import SubtaskCreate
from fractal.server.app.models import Task
from fractal.server.app.models import TaskCreate
from fractal.server.tasks import collect_tasks
from fractal.tasks import __FRACTAL_MANIFEST__

N_CORE_TASKS = len(__FRACTAL_MANIFEST__)


async def test_collection(db, client, MockCurrentUser):
    """
    GIVEN a running server
    WHEN the `POST task/collect/` endpoint is called
    THEN the table `Task` is updated accordingly, collecting the available
         tasks
    """
    res = await db.execute(select(Task))
    n_tasks = len(res.scalars().all())  # FIXME: run query server side!
    assert n_tasks == 0

    n_target = len(list(collect_tasks()))

    async with MockCurrentUser(persist=True):
        res = await client.post("api/v1/task/collect/")
        assert res.status_code == 201
        data = res.json()
        debug(data)
        assert data["inserted"] == N_CORE_TASKS
        assert data["updated"] == 0

    res = await db.execute(select(Task))
    n_tasks = len(res.scalars().all())  # FIXME: run query server side!
    assert n_tasks == n_target

    # Check for idempotency
    async with MockCurrentUser(persist=True):
        res = await client.post("api/v1/task/collect/")
        data = res.json()
        assert res.status_code == 201
        assert data["inserted"] == 0
        assert data["updated"] == N_CORE_TASKS

    res = await db.execute(select(Task))
    n_tasks = len(res.scalars().all())  # FIXME: run query server side!
    assert n_tasks == n_target


async def test_task_get_list(db, client, task_factory, MockCurrentUser):
    t1 = await task_factory(index=1)
    await task_factory(index=2, subtask_list=[t1])

    async with MockCurrentUser(persist=True):
        res = await client.get("api/v1/task/")
        data = res.json()
        assert res.status_code == 200
        debug(data)
        assert len(data) == 2
        assert data[1]["id"] == 2
        assert data[1]["subtask_list"][0]["subtask"]["id"] == t1.id


async def test_task_create(db, client, MockCurrentUser):
    """
    GIVEN a CreateTask object
    WHEN it is fed to the `POST task` endpoint
    THEN a new task is correctly created
    """
    task = TaskCreate(
        name="mytask",
        resource_type="workflow",
        input_type="Any",
        output_type="Any",
    )
    async with MockCurrentUser(persist=True):
        res = await client.post("api/v1/task/", json=task.dict())
        debug(res.json())
        data = res.json()
        for key, item in task.dict().items():
            assert data[key] == item


async def test_subtask_create(db, client, MockCurrentUser, task_factory):
    """
    GIVEN two tasks `mother` and `daugher` in the database
    WHEN the create subtask endpoint is called to add `daughter` as subtask of
         `mother`
    THEN `daughter` is correctly added as subtask of `mother`
    """
    t0 = await task_factory(name="mother")
    t1 = await task_factory(name="daughter")

    debug(t0)
    debug(t1)

    assert t0.subtask_list == []

    subtask = SubtaskCreate(subtask_id=t1.id)

    async with MockCurrentUser(persist=True):
        res = await client.post(
            f"api/v1/task/{t0.id}/subtask/", json=subtask.dict()
        )
        debug(res.json())
        data = res.json()

    assert data["subtask_list"][0]["subtask_id"] == t1.id
    data_subtask = data["subtask_list"][0]["subtask"]
    t1_dict = t1.dict()
    for key, value in data_subtask.items():
        value == t1_dict[key]
