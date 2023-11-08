PREFIX = "api/v1"


async def test_unauthorized_to_monitor(client, MockCurrentUser):

    async with MockCurrentUser(
        persist=True, user_kwargs={"is_superuser": False}
    ):
        res = await client.get(f"{PREFIX}/monitoring/project/")
        assert res.status_code == 403
        res = await client.get(f"{PREFIX}/monitoring/workflow/")
        assert res.status_code == 403
        res = await client.get(f"{PREFIX}/monitoring/dataset/")
        assert res.status_code == 403
        res = await client.get(f"{PREFIX}/monitoring/job/")
        assert res.status_code == 403

    async with MockCurrentUser(
        persist=True, user_kwargs={"is_superuser": True}
    ):
        res = await client.get(f"{PREFIX}/monitoring/project/")
        assert res.status_code == 200
        res = await client.get(f"{PREFIX}/monitoring/workflow/")
        assert res.status_code == 200
        res = await client.get(f"{PREFIX}/monitoring/dataset/")
        assert res.status_code == 200
        res = await client.get(f"{PREFIX}/monitoring/job/")
        assert res.status_code == 200


async def test_monitor_project(client, MockCurrentUser, project_factory):

    async with MockCurrentUser(
        persist=True, user_kwargs={"is_superuser": True}
    ):
        res = await client.get(f"{PREFIX}/monitoring/project/")
        assert res.status_code == 200
        assert res.json() == []

    async with MockCurrentUser(
        persist=True, user_kwargs={"is_superuser": False}
    ) as user:
        project1 = await project_factory(user)
        prj1_id = project1.id
        await project_factory(user)

    async with MockCurrentUser(
        persist=True, user_kwargs={"is_superuser": True}
    ):
        res = await client.get(f"{PREFIX}/monitoring/project/")
        assert res.status_code == 200
        assert len(res.json()) == 2
        res = await client.get(f"{PREFIX}/monitoring/project/?id={prj1_id}")
        assert res.status_code == 200
        assert len(res.json()) == 1


async def test_monitor_workflow(
    client, MockCurrentUser, project_factory, workflow_factory
):

    async with MockCurrentUser(
        persist=True, user_kwargs={"is_superuser": False}
    ) as user:

        project1 = await project_factory(user)
        workflow1a = await workflow_factory(
            project_id=project1.id, name="Workflow 1a"
        )
        workflow1b = await workflow_factory(
            project_id=project1.id, name="Workflow 1b"
        )

        project2 = await project_factory(user)
        workflow2a = await workflow_factory(
            project_id=project2.id, name="Workflow 2a"
        )

    async with MockCurrentUser(
        persist=True, user_kwargs={"is_superuser": True}
    ):
        # get all workflow
        res = await client.get(f"{PREFIX}/monitoring/workflow/")
        assert res.status_code == 200
        assert len(res.json()) == 3

        # get workflow by id
        res = await client.get(
            f"{PREFIX}/monitoring/workflow/?id={workflow1a.id}"
        )
        assert res.status_code == 200
        assert len(res.json()) == 1
        assert res.json()[0]["name"] == workflow1a.name

        # get workflow by project_id
        res = await client.get(
            f"{PREFIX}/monitoring/workflow/?project_id={project1.id}"
        )
        assert res.status_code == 200
        assert len(res.json()) == 2

        # get workflow by project_id and id
        res = await client.get(
            f"{PREFIX}/monitoring/workflow/"
            f"?project_id={project1.id}&id={workflow1b.id}"
        )
        assert res.status_code == 200
        assert len(res.json()) == 1
        assert res.json()[0]["name"] == workflow1b.name

        res = await client.get(
            f"{PREFIX}/monitoring/workflow/"
            f"?project_id={project1.id}&id={workflow2a.id}"
        )
        assert res.status_code == 200
        assert len(res.json()) == 0

        # get workflow by name
        res = await client.get(
            f"{PREFIX}/monitoring/workflow/?name={workflow2a.name}"
        )
        assert res.status_code == 200
        assert len(res.json()) == 1
        assert res.json()[0]["name"] == workflow2a.name

        res = await client.get(f"{PREFIX}/monitoring/workflow/?name=Workflow")
        assert res.status_code == 200
        assert len(res.json()) == 3
