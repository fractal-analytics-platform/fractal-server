import pytest
from devtools import debug
from sqlmodel import select

from fractal_server.app.models import ApplyWorkflow
from fractal_server.app.models import Dataset
from fractal_server.app.models import Project

PREFIX = "/api/v1"


async def test_get_project(client, db, project_factory, MockCurrentUser):
    # unauthenticated
    res = await client.get(f"{PREFIX}/project/")
    assert res.status_code == 401

    # authenticated
    async with MockCurrentUser(persist=True) as user:
        other_project = await project_factory(user)

    async with MockCurrentUser(persist=True) as user:
        res = await client.get(f"{PREFIX}/project/")
        debug(res)
        assert res.status_code == 200
        assert res.json() == []

        await project_factory(user)
        res = await client.get(f"{PREFIX}/project/")
        data = res.json()
        debug(data)
        assert res.status_code == 200
        assert len(data) == 1

        project_id = data[0]["id"]
        res = await client.get(f"{PREFIX}/project/{project_id}")
        assert res.status_code == 200
        assert res.json()["id"] == project_id

        # fail on non existent project
        res = await client.get(f"{PREFIX}/project/123456")
        assert res.status_code == 404

        # fail on other owner's project
        res = await client.get(f"{PREFIX}/project/{other_project.id}")
        assert res.status_code == 403


async def test_post_project(app, client, MockCurrentUser, db):
    payload = dict(name="new project")

    # Fail for anonymous user
    res = await client.post(f"{PREFIX}/project/", json=payload)
    data = res.json()
    assert res.status_code == 401

    async with MockCurrentUser(persist=True):
        res = await client.post(f"{PREFIX}/project/", json=payload)
        data = res.json()
        assert res.status_code == 201
        debug(data)
        assert data["name"] == payload["name"]

        # Payload without `name`
        empty_payload = {}
        res = await client.post(f"{PREFIX}/project/", json=empty_payload)
        debug(res.json())
        assert res.status_code == 422


async def test_post_project_name_constraint(app, client, MockCurrentUser, db):
    payload = dict(name="new project")
    res = await client.post(f"{PREFIX}/project/", json=payload)
    assert res.status_code == 401

    async with MockCurrentUser(persist=True):
        # Create a first project named "new project"
        res = await client.post(f"{PREFIX}/project/", json=payload)
        assert res.status_code == 201

        # Create a second project named "new project", and check that this
        # fails with 422_UNPROCESSABLE_ENTITY
        res = await client.post(f"{PREFIX}/project/", json=payload)
        assert res.status_code == 422


async def test_patch_project_name_constraint(app, client, MockCurrentUser, db):
    async with MockCurrentUser(persist=True):
        # Create a first project named "name1"
        res = await client.post(f"{PREFIX}/project/", json=dict(name="name1"))
        assert res.status_code == 201

        # Create a second project named "name2"
        res = await client.post(f"{PREFIX}/project/", json=dict(name="name2"))
        assert res.status_code == 201
        prj2 = res.json()

        # Fail in editing the name of prj2 to "name1"
        res = await client.patch(
            f"{PREFIX}/project/{prj2['id']}", json=dict(name="name1")
        )
        assert res.status_code == 422
        assert res.json()["detail"] == "Project name (name1) already in use"

    async with MockCurrentUser(persist=True):
        # Using another user, create a project named "name3"
        res = await client.post(f"{PREFIX}/project/", json=dict(name="name3"))
        assert res.status_code == 201
        prj3 = res.json()
        # Edit the name of prj3 to "name1" without errors
        res = await client.patch(
            f"{PREFIX}/project/{prj3['id']}", json=dict(name="name1")
        )
        debug(res.json())
        assert res.status_code == 200


@pytest.mark.parametrize("new_name", (None, "new name"))
@pytest.mark.parametrize("new_read_only", (None, True, False))
async def test_patch_project(
    new_name,
    new_read_only,
    app,
    client,
    MockCurrentUser,
    db,
):
    """
    Test that the project can be patched correctly, with any possible
    combination of set/unset attributes.
    """
    async with MockCurrentUser(persist=True):
        # Create project
        payload = dict(
            name="old name",
            read_only=True,
        )
        res = await client.post(f"{PREFIX}/project/", json=payload)
        old_project = res.json()
        project_id = old_project["id"]
        assert res.status_code == 201

        # Patch project
        payload = {}
        if new_name:
            payload["name"] = new_name
        if new_read_only:
            payload["read_only"] = new_read_only
        debug(payload)
        res = await client.patch(
            f"{PREFIX}/project/{project_id}", json=payload
        )
        new_project = res.json()
        debug(new_project)
        assert res.status_code == 200
        for key, value in new_project.items():
            if key in payload.keys():
                assert value == payload[key]
            else:
                assert value == old_project[key]


async def test_delete_project(
    client,
    MockCurrentUser,
    db,
    job_factory,
    dataset_factory,
    workflow_factory,
    tmp_path,
    task_factory,
):
    async with MockCurrentUser(persist=True):
        res = await client.get(f"{PREFIX}/project/")
        data = res.json()
        assert len(data) == 0

        # Create a project
        res = await client.post(f"{PREFIX}/project/", json=dict(name="name"))
        p = res.json()

        # Verify that the project was created
        res = await client.get(f"{PREFIX}/project/")
        data = res.json()
        debug(data)
        assert res.status_code == 200
        assert len(data) == 1
        project_id = res.json()[0]["id"]

        # Add a dataset to the project
        dataset = await dataset_factory(project_id=project_id)
        dataset_id = dataset.id

        # Add a workflow to the project
        wf = await workflow_factory(project_id=p["id"])
        t = await task_factory()
        await wf.insert_task(task_id=t.id, db=db)

        # Add a job to the project
        await job_factory(
            project_id=p["id"],
            workflow_id=wf.id,
            working_dir=(tmp_path / "some_working_dir").as_posix(),
            input_dataset_id=dataset_id,
            output_dataset_id=dataset_id,
        )

        # Check that a project-related job exists
        stm = select(ApplyWorkflow).join(Project).where(Project.id == p["id"])
        res = await db.execute(stm)
        jobs = list(res)
        assert len(jobs) == 1

        # Delete the project
        res = await client.delete(f"{PREFIX}/project/{p['id']}")
        assert res.status_code == 204

        # Check that the project was deleted
        res = await client.get(f"{PREFIX}/project/")
        data = res.json()
        assert len(data) == 0

        # Check that project-related datasets were deleted
        stm = select(Dataset).join(Project).where(Project.id == p["id"])
        res = await db.execute(stm)
        datasets = list(res)
        debug(datasets)
        assert len(datasets) == 0

        # Check that project-related jobs were deleted
        stm = select(ApplyWorkflow).join(Project).where(Project.id == p["id"])
        jobs = await db.execute(stm)
        jobs = list(jobs)
        debug(jobs)
        assert len(jobs) == 0
