from devtools import debug


PREFIX = "/api/v1/project"


async def test_project_get(client, db, project_factory, MockCurrentUser):
    # unauthenticated

    res = await client.get(f"{PREFIX}/")
    assert res.status_code == 401

    # authenticated
    async with MockCurrentUser(persist=True) as user:
        other_project = await project_factory(user)

    async with MockCurrentUser(persist=True) as user:
        res = await client.get(f"{PREFIX}/")
        debug(res)
        assert res.status_code == 200
        assert res.json() == []

        await project_factory(user)
        res = await client.get(f"{PREFIX}/")
        data = res.json()
        debug(data)
        assert res.status_code == 200
        assert len(data) == 1

        project_id = data[0]["id"]
        res = await client.get(f"{PREFIX}/{project_id}")
        assert res.status_code == 200
        assert res.json()["id"] == project_id

        # fail on non existent project
        res = await client.get(f"{PREFIX}/666")
        assert res.status_code == 404

        # fail on other owner's project
        res = await client.get(f"{PREFIX}/{other_project.id}")
        assert res.status_code == 403


async def test_project_creation(app, client, MockCurrentUser, db):
    payload = dict(
        name="new project",
        project_dir="/some/path/",
    )
    res = await client.post(f"{PREFIX}/", json=payload)
    data = res.json()
    assert res.status_code == 401

    async with MockCurrentUser(persist=True):
        res = await client.post(f"{PREFIX}/", json=payload)
        data = res.json()
        assert res.status_code == 201
        debug(data)
        assert data["name"] == payload["name"]
        assert data["project_dir"] == payload["project_dir"]


async def test_project_creation_name_constraint(
    app, client, MockCurrentUser, db
):
    payload = dict(
        name="new project",
        project_dir="/some/path/",
    )
    res = await client.post(f"{PREFIX}/", json=payload)
    assert res.status_code == 401

    async with MockCurrentUser(persist=True):

        # Create a first project named "new project"
        res = await client.post(f"{PREFIX}/", json=payload)
        assert res.status_code == 201

        # Create a second project named "new project", and check that this
        # fails with 422_UNPROCESSABLE_ENTITY
        res = await client.post(f"{PREFIX}/", json=payload)
        assert res.status_code == 422


async def test_add_dataset(app, client, MockCurrentUser, db):

    async with MockCurrentUser(persist=True):

        # CREATE A PROJECT

        res = await client.post(
            f"{PREFIX}/",
            json=dict(
                name="test project",
                project_dir="/tmp/",
            ),
        )
        assert res.status_code == 201
        project = res.json()
        project_id = project["id"]

        # ADD DATASET

        payload = dict(
            name="new dataset",
            project_id=project_id,
            resource_list=["./test"],
            meta={"xy": 2},
        )
        res = await client.post(
            f"{PREFIX}/{project_id}/",
            json=payload,
        )
        assert res.status_code == 201
        dataset = res.json()
        assert dataset["name"] == payload["name"]
        assert dataset["project_id"] == payload["project_id"]
        assert dataset["meta"] == payload["meta"]

        # EDIT DATASET

        payload = dict(name="new dataset name", meta={})
        res = await client.patch(
            f"{PREFIX}/{project_id}/{dataset['id']}",
            json=payload,
        )
        patched_dataset = res.json()
        debug(patched_dataset)
        assert res.status_code == 200
        for k, v in payload.items():
            assert patched_dataset[k] == payload[k]


async def test_delete_project(client, MockCurrentUser, project_factory):

    async with MockCurrentUser(persist=True) as user:
        res = await client.get(f"{PREFIX}/")
        data = res.json()
        assert len(data) == 0

        # CREATE A PRJ
        p = await project_factory(user)

        res = await client.get(f"{PREFIX}/")
        data = res.json()
        debug(data)

        assert res.status_code == 200
        assert len(data) == 1

        # DELETE PRJ
        res = await client.delete(f"{PREFIX}/{p.id}")
        assert res.status_code == 204

        # GET LIST again and check that it is empty
        res = await client.get(f"{PREFIX}/")
        data = res.json()
        assert len(data) == 0
