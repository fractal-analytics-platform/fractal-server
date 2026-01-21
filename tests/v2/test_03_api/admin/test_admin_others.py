from sqlalchemy import select

from fractal_server.app.models import Resource
from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)

PREFIX = "/admin/v2"


async def test_unauthorized_to_admin(client, MockCurrentUser):
    async with MockCurrentUser(is_superuser=False):
        res = await client.get(f"{PREFIX}/job/")
        assert res.status_code == 401

    async with MockCurrentUser(is_superuser=True):
        res = await client.get(f"{PREFIX}/job/")
        assert res.status_code == 200


async def test_task_query(
    db,
    client,
    MockCurrentUser,
    project_factory,
    workflow_factory,
    task_factory,
):
    async with MockCurrentUser(is_superuser=True) as user:
        project = await project_factory(user)

        workflow1 = await workflow_factory(project_id=project.id)
        workflow2 = await workflow_factory(project_id=project.id)

        task1 = await task_factory(
            user_id=user.id,
            name="Foo",
            category="Conversion",
            modality="HCS",
            authors="Name1 Surname1,Name2 Surname2...",
        )
        task2 = await task_factory(
            user_id=user.id,
            name="abcdef",
            category="Conversion",
            modality="EM",
            authors="Name1 Surname3,Name3 Surname2...",
        )
        task3 = await task_factory(user_id=user.id, index=3, modality="EM")

        # task1 to workflow 1 and 2
        await _workflow_insert_task(
            workflow_id=workflow1.id, task_id=task1.id, db=db
        )
        await _workflow_insert_task(
            workflow_id=workflow2.id, task_id=task1.id, db=db
        )
        # task2 to workflow2
        await _workflow_insert_task(
            workflow_id=workflow2.id, task_id=task2.id, db=db
        )
        # task3 is orphan

        # Query all Tasks
        res = await client.get(f"{PREFIX}/task/")
        assert res.status_code == 200
        assert res.json()["total_count"] == 3

        # Query Tasks with given type
        res = await client.get(
            f"{PREFIX}/task/?task_type=converter_non_parallel"
        )
        assert res.status_code == 200
        assert res.json()["total_count"] == 0

        # Query Tasks with given type
        res = await client.get(f"{PREFIX}/task/?task_type=compound")
        assert res.status_code == 200
        assert res.json()["total_count"] == 3

        # Query first page of all Tasks
        res = await client.get(f"{PREFIX}/task/?page_size=1&page=1")
        assert res.status_code == 200
        assert res.json()["total_count"] == 3
        assert len(res.json()["items"]) == 1

        # Query all tasks, with naive `resource_id` query parameter
        # (assuming a single resource exists in the db)
        res = await db.execute(select(Resource.id))
        resource_id = res.scalars().first()
        res = await client.get(f"{PREFIX}/task/?{resource_id=}")
        assert res.status_code == 200
        assert len(res.json()["items"]) == 3

        # Query by ID

        res = await client.get(f"{PREFIX}/task/?id={task1.id}")
        assert len(res.json()["items"]) == 1
        assert (
            res.json()["items"][0]["task"].items() <= task1.model_dump().items()
        )
        assert len(res.json()["items"][0]["relationships"]) == 2
        _common_args = dict(
            project_id=project.id,
            project_name=project.name,
            project_users=[dict(id=user.id, email=user.email)],
        )
        assert res.json()["items"][0]["relationships"][0] == dict(
            workflow_id=workflow1.id,
            workflow_name=workflow1.name,
            **_common_args,
        )
        assert res.json()["items"][0]["relationships"][1] == dict(
            workflow_id=workflow2.id,
            workflow_name=workflow2.name,
            **_common_args,
        )

        res = await client.get(f"{PREFIX}/task/?id={task2.id}")
        assert len(res.json()["items"]) == 1
        assert res.json()["items"][0]["task"]["id"] == task2.id
        assert len(res.json()["items"][0]["relationships"]) == 1

        res = await client.get(f"{PREFIX}/task/?id={task3.id}")
        assert len(res.json()["items"]) == 1
        assert res.json()["items"][0]["task"]["id"] == task3.id
        assert len(res.json()["items"][0]["relationships"]) == 0

        res = await client.get(f"{PREFIX}/task/?id=1000")
        assert len(res.json()["items"]) == 0

        # Query by VERSION

        res = await client.get(f"{PREFIX}/task/?version=0")  # task 1 + 2
        assert len(res.json()["items"]) == 2

        res = await client.get(f"{PREFIX}/task/?version=3")  # task 3
        assert len(res.json()["items"]) == 1

        res = await client.get(f"{PREFIX}/task/?version=1.2")
        assert len(res.json()["items"]) == 0

        # Query by NAME

        res = await client.get(f"{PREFIX}/task/?name={task1.name}")
        assert len(res.json()["items"]) == 1

        res = await client.get(f"{PREFIX}/task/?name={task2.name}")
        assert len(res.json()["items"]) == 1

        res = await client.get(f"{PREFIX}/task/?name={task3.name}")
        assert len(res.json()["items"]) == 1

        res = await client.get(f"{PREFIX}/task/?name=nonamelikethis")
        assert len(res.json()["items"]) == 0

        res = await client.get(f"{PREFIX}/task/?name=f")  # task 1 + 2
        assert len(res.json()["items"]) == 2

        res = await client.get(f"{PREFIX}/task/?name=F")  # task 1 + 2
        assert len(res.json()["items"]) == 2

        # Query by CATEGORY

        res = await client.get(f"{PREFIX}/task/?category=Conversion")
        assert len(res.json()["items"]) == 2
        res = await client.get(f"{PREFIX}/task/?category=conversion")
        assert len(res.json()["items"]) == 2
        res = await client.get(f"{PREFIX}/task/?category=conversio")
        assert len(res.json()["items"]) == 0

        # Query by MODALITY

        res = await client.get(f"{PREFIX}/task/?modality=HCS")
        assert len(res.json()["items"]) == 1
        res = await client.get(f"{PREFIX}/task/?modality=em")
        assert len(res.json()["items"]) == 2
        res = await client.get(f"{PREFIX}/task/?modality=foo")
        assert len(res.json()["items"]) == 0

        # Query by AUTHOR

        res = await client.get(f"{PREFIX}/task/?author=name1")
        assert len(res.json()["items"]) == 2
        res = await client.get(f"{PREFIX}/task/?author=surname1")
        assert len(res.json()["items"]) == 1
        res = await client.get(f"{PREFIX}/task/?author=,")
        assert len(res.json()["items"]) == 2

        # --------------------------
        # Relationships after deleting the Project

        res = await client.delete(f"api/v2/project/{project.id}/")
        assert res.status_code == 204

        # Query by ID

        for t in [task1, task2, task3]:
            res = await client.get(f"{PREFIX}/task/?id={t.id}")
            assert len(res.json()["items"]) == 1
            assert (
                res.json()["items"][0]["task"].items() <= t.model_dump().items()
            )
            assert res.json()["items"][0]["task"]["id"] == t.id
            assert len(res.json()["items"][0]["relationships"]) == 0
