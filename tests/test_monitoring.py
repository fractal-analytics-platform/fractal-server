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
        # get all workflows
        res = await client.get(f"{PREFIX}/monitoring/workflow/")
        assert res.status_code == 200
        assert len(res.json()) == 3

        # get workflows by id
        res = await client.get(
            f"{PREFIX}/monitoring/workflow/?id={workflow1a.id}"
        )
        assert res.status_code == 200
        assert len(res.json()) == 1
        assert res.json()[0]["name"] == workflow1a.name

        # get workflows by project_id
        res = await client.get(
            f"{PREFIX}/monitoring/workflow/?project_id={project1.id}"
        )
        assert res.status_code == 200
        assert len(res.json()) == 2

        # get workflows by project_id and id
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

        # get workflows by name
        res = await client.get(
            f"{PREFIX}/monitoring/workflow/?name={workflow2a.name}"
        )
        assert res.status_code == 200
        assert len(res.json()) == 1
        assert res.json()[0]["name"] == workflow2a.name

        res = await client.get(f"{PREFIX}/monitoring/workflow/?name=Workflow")
        assert res.status_code == 200
        assert len(res.json()) == 3


async def test_monitor_dataset(
    client, MockCurrentUser, project_factory, workflow_factory, dataset_factory
):

    async with MockCurrentUser(
        persist=True, user_kwargs={"is_superuser": False}
    ) as user:

        project1 = await project_factory(user)

        ds1a = await dataset_factory(
            project_id=project1.id,
            name="ds1a",
            type="zarr",
            read_only=False,
        )
        await dataset_factory(
            project_id=project1.id,
            name="ds1b",
            type="image",
            read_only=True,
        )

        project2 = await project_factory(user)

        await dataset_factory(
            project_id=project2.id,
            name="ds2a",
            type="zarr",
            read_only=True,
        )

    async with MockCurrentUser(
        persist=True, user_kwargs={"is_superuser": True}
    ):
        # get all datasets
        res = await client.get(f"{PREFIX}/monitoring/dataset/")
        assert res.status_code == 200
        assert len(res.json()) == 3

        # get datasets by id
        res = await client.get(f"{PREFIX}/monitoring/dataset/?id={ds1a.id}")
        assert res.status_code == 200
        assert len(res.json()) == 1
        assert res.json()[0]["name"] == ds1a.name
        res = await client.get(f"{PREFIX}/monitoring/dataset/?id=123456789")
        assert res.status_code == 200
        assert len(res.json()) == 0

        # get datasets by project_id
        res = await client.get(
            f"{PREFIX}/monitoring/dataset/?project_id={project1.id}"
        )
        assert res.status_code == 200
        assert len(res.json()) == 2
        res = await client.get(
            f"{PREFIX}/monitoring/dataset/?project_id={project2.id}"
        )
        assert res.status_code == 200
        assert len(res.json()) == 1

        # get datasets by name
        res = await client.get(
            f"{PREFIX}/monitoring/dataset/?project_id={project1.id}&name=a"
        )
        assert res.status_code == 200
        assert len(res.json()) == 1
        assert res.json()[0]["name"] == ds1a.name
        res = await client.get(
            f"{PREFIX}/monitoring/dataset/?project_id={project1.id}&name=c"
        )
        assert res.status_code == 200
        assert len(res.json()) == 0

        # get datasets by type and read_only
        res = await client.get(f"{PREFIX}/monitoring/dataset/?type=zarr")
        assert res.status_code == 200
        assert len(res.json()) == 2
        res = await client.get(
            f"{PREFIX}/monitoring/dataset/?type=zarr&read_only=true"
        )
        assert res.status_code == 200
        assert len(res.json()) == 1
        res = await client.get(f"{PREFIX}/monitoring/dataset/?read_only=true")
        assert res.status_code == 200
        assert len(res.json()) == 2
