from devtools import debug
from sqlalchemy import select

from fractal_server.app.models import Resource
from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)

PREFIX = "/admin/v2"


async def test_unauthorized_to_admin(client, MockCurrentUser):
    async with MockCurrentUser(user_kwargs={"is_superuser": False}):
        res = await client.get(f"{PREFIX}/job/")
        assert res.status_code == 401

    async with MockCurrentUser(user_kwargs={"is_superuser": True}):
        res = await client.get(f"{PREFIX}/job/")
        assert res.status_code == 200


async def test_view_project(client, MockCurrentUser, project_factory_v2):
    async with MockCurrentUser(
        user_kwargs={"is_superuser": True}
    ) as superuser:
        res = await client.get(f"{PREFIX}/project/")
        assert res.status_code == 200
        assert res.json() == []
        await project_factory_v2(superuser)

    async with MockCurrentUser(user_kwargs={"is_superuser": False}) as user:
        project = await project_factory_v2(user)
        prj_id = project.id
        await project_factory_v2(user)
        user_id = user.id

    async with MockCurrentUser(user_kwargs={"is_superuser": True}):
        res = await client.get(f"{PREFIX}/project/")
        assert res.status_code == 200
        assert len(res.json()) == 3
        res = await client.get(f"{PREFIX}/project/?id={prj_id}")
        assert res.status_code == 200
        assert len(res.json()) == 1
        res = await client.get(f"{PREFIX}/project/?user_id={user_id}")
        assert res.status_code == 200
        assert len(res.json()) == 2
        res = await client.get(
            f"{PREFIX}/project/?user_id={user_id}&id={prj_id}"
        )
        assert res.status_code == 200
        assert len(res.json()) == 1

        res = await client.get(f"{PREFIX}/project/?id=9999999")
        assert res.status_code == 200
        assert res.json() == []
        res = await client.get(f"{PREFIX}/project/?user_id=9999999")
        assert res.status_code == 200
        assert res.json() == []


async def test_task_query(
    db,
    client,
    MockCurrentUser,
    project_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
):
    async with MockCurrentUser(user_kwargs={"is_superuser": True}) as user:
        project = await project_factory_v2(user)

        workflow1 = await workflow_factory_v2(project_id=project.id)
        workflow2 = await workflow_factory_v2(project_id=project.id)

        task1 = await task_factory_v2(
            user_id=user.id,
            name="Foo",
            source="xxx",
            category="Conversion",
            modality="HCS",
            authors="Name1 Surname1,Name2 Surname2...",
        )
        task2 = await task_factory_v2(
            user_id=user.id,
            name="abcdef",
            source="yyy",
            category="Conversion",
            modality="EM",
            authors="Name1 Surname3,Name3 Surname2...",
        )
        task3 = await task_factory_v2(
            user_id=user.id, index=3, source="source3", modality="EM"
        )

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

        # Query ALL Tasks

        res = await client.get(f"{PREFIX}/task/")
        assert res.status_code == 200
        assert len(res.json()) == 3
        debug(res.json())

        # Query all tasks, with naive `resource_id` query parameter
        # (assuming a single resource exists in the db)
        res = await db.execute(select(Resource.id))
        resource_id = res.scalars().first()
        res = await client.get(f"{PREFIX}/task/?{resource_id=}")
        assert res.status_code == 200
        assert len(res.json()) == 3

        # Query by ID

        res = await client.get(f"{PREFIX}/task/?id={task1.id}")
        assert len(res.json()) == 1
        assert res.json()[0]["task"].items() <= task1.model_dump().items()
        assert len(res.json()[0]["relationships"]) == 2
        _common_args = dict(
            project_id=project.id,
            project_name=project.name,
            project_users=[dict(id=user.id, email=user.email)],
        )
        assert res.json()[0]["relationships"][0] == dict(
            workflow_id=workflow1.id,
            workflow_name=workflow1.name,
            **_common_args,
        )
        assert res.json()[0]["relationships"][1] == dict(
            workflow_id=workflow2.id,
            workflow_name=workflow2.name,
            **_common_args,
        )

        res = await client.get(f"{PREFIX}/task/?id={task2.id}")
        assert len(res.json()) == 1
        assert res.json()[0]["task"]["id"] == task2.id
        assert len(res.json()[0]["relationships"]) == 1

        res = await client.get(f"{PREFIX}/task/?id={task3.id}")
        assert len(res.json()) == 1
        assert res.json()[0]["task"]["id"] == task3.id
        assert len(res.json()[0]["relationships"]) == 0

        res = await client.get(f"{PREFIX}/task/?id=1000")
        assert len(res.json()) == 0

        # Query by SOURCE

        res = await client.get(f"{PREFIX}/task/?source={task1.source}")
        assert len(res.json()) == 1
        assert res.json()[0]["task"]["id"] == task1.id
        assert len(res.json()[0]["relationships"]) == 2

        res = await client.get(f"{PREFIX}/task/?source={task1.source[0]}")
        assert len(res.json()) == 1
        assert res.json()[0]["task"]["id"] == task1.id
        assert len(res.json()[0]["relationships"]) == 2

        res = await client.get(f"{PREFIX}/task/?source={task2.source}")
        assert len(res.json()) == 1
        assert res.json()[0]["task"]["id"] == task2.id
        assert len(res.json()[0]["relationships"]) == 1

        res = await client.get(f"{PREFIX}/task/?source={task3.source}")
        assert len(res.json()) == 1
        assert res.json()[0]["task"]["id"] == task3.id
        assert len(res.json()[0]["relationships"]) == 0

        res = await client.get(f"{PREFIX}/task/?source=foo")
        assert len(res.json()) == 0

        # Query by VERSION

        res = await client.get(f"{PREFIX}/task/?version=0")  # task 1 + 2
        assert len(res.json()) == 2

        res = await client.get(f"{PREFIX}/task/?version=3")  # task 3
        assert len(res.json()) == 1

        res = await client.get(f"{PREFIX}/task/?version=1.2")
        assert len(res.json()) == 0

        # Query by NAME

        res = await client.get(f"{PREFIX}/task/?name={task1.name}")
        assert len(res.json()) == 1

        res = await client.get(f"{PREFIX}/task/?name={task2.name}")
        assert len(res.json()) == 1

        res = await client.get(f"{PREFIX}/task/?name={task3.name}")
        assert len(res.json()) == 1

        res = await client.get(f"{PREFIX}/task/?name=nonamelikethis")
        assert len(res.json()) == 0

        res = await client.get(f"{PREFIX}/task/?name=f")  # task 1 + 2
        assert len(res.json()) == 2

        res = await client.get(f"{PREFIX}/task/?name=F")  # task 1 + 2
        assert len(res.json()) == 2

        # Query by CATEGORY

        res = await client.get(f"{PREFIX}/task/?category=Conversion")
        assert len(res.json()) == 2
        res = await client.get(f"{PREFIX}/task/?category=conversion")
        assert len(res.json()) == 2
        res = await client.get(f"{PREFIX}/task/?category=conversio")
        assert len(res.json()) == 0

        # Query by MODALITY

        res = await client.get(f"{PREFIX}/task/?modality=HCS")
        assert len(res.json()) == 1
        res = await client.get(f"{PREFIX}/task/?modality=em")
        assert len(res.json()) == 2
        res = await client.get(f"{PREFIX}/task/?modality=foo")
        assert len(res.json()) == 0

        # Query by AUTHOR

        res = await client.get(f"{PREFIX}/task/?author=name1")
        assert len(res.json()) == 2
        res = await client.get(f"{PREFIX}/task/?author=surname1")
        assert len(res.json()) == 1
        res = await client.get(f"{PREFIX}/task/?author=,")
        assert len(res.json()) == 2

        # --------------------------
        # Relationships after deleting the Project

        res = await client.delete(f"api/v2/project/{project.id}/")
        assert res.status_code == 204

        # Query by ID

        for t in [task1, task2, task3]:
            res = await client.get(f"{PREFIX}/task/?id={t.id}")
            assert len(res.json()) == 1
            assert res.json()[0]["task"].items() <= t.model_dump().items()
            assert res.json()[0]["task"]["id"] == t.id
            assert len(res.json()[0]["relationships"]) == 0

        # Query by SOURCE
        for t in [task1, task2, task3]:
            res = await client.get(f"{PREFIX}/task/?source={t.source}")
            assert len(res.json()) == 1
            assert res.json()[0]["task"]["id"] == t.id
            assert len(res.json()[0]["relationships"]) == 0

        # --------------------------
        # Too many Tasks

        # We need 'db.close' to avoid: "<sqlalchemy.exc.SAWarning: Identity map
        # already had an identity for (<class 'fractal_server.app.models.v2.*'>
        # ,(1,), None), replacing it with newly flushed object." where * is in
        # [project.ProjectV2, workflow.WorkflowV2, workflowtask.WorkflowTaskV2]
        await db.close()

        new_project = await project_factory_v2(user)
        new_workflow = await workflow_factory_v2(project_id=new_project.id)

        for i in range(2):
            task = await task_factory_v2(
                user_id=user.id, name=f"n{i}", source=f"s{i}"
            )
            await _workflow_insert_task(
                workflow_id=new_workflow.id, task_id=task.id, db=db
            )
        res = await client.get(f"{PREFIX}/task/?max_number_of_results=1")
        assert res.status_code == 422
        assert "Please add more query filters" in res.json()["detail"]
