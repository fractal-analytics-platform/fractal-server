from pathlib import Path
from zipfile import ZipFile

from devtools import debug
from sqlmodel import select

from fractal_server.app.models import Dataset
from fractal_server.app.models import Project
from fractal_server.app.models import Resource


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

        # ADD RESOURCE TO DATASET

        payload = dict(path="/some/absolute/path", glob_pattern="*.png")
        res = await client.post(
            f"{PREFIX}/{project_id}/{dataset['id']}",
            json=payload,
        )
        assert res.status_code == 201
        resource = res.json()
        debug(resource)
        assert resource["path"] == payload["path"]


async def test_dataset_get(app, client, MockCurrentUser, db):

    async with MockCurrentUser(persist=True):

        # Create a project
        res = await client.post(
            f"{PREFIX}/",
            json=dict(
                name="test project",
                project_dir="/tmp/",
            ),
        )
        assert res.status_code == 201
        project = res.json()
        debug(project)
        project_id = project["id"]
        dataset_id = project["dataset_list"][0]["id"]

        # Show existing dataset
        res = await client.get(
            f"{PREFIX}/{project_id}/{dataset_id}",
        )
        assert res.status_code == 200
        dataset = res.json()
        debug(dataset)
        assert dataset["project_id"] == project_id

        # Show missing dataset
        invalid_dataset_id = 999
        res = await client.get(
            f"{PREFIX}/{project_id}/{invalid_dataset_id}",
        )
        assert res.status_code == 404


async def test_add_dataset_local_path_error(app, client, MockCurrentUser, db):

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

        # ADD WRONG RESOURCE TO DATASET

        payload = dict(path="some/local/path", glob_pattern="*.png")
        debug(payload["path"])

        res = await client.post(
            f"{PREFIX}/{project_id}/{dataset['id']}",
            json=payload,
        )
        assert res.status_code == 422


async def test_delete_project(client, MockCurrentUser, db):

    async with MockCurrentUser(persist=True):
        res = await client.get(f"{PREFIX}/")
        data = res.json()
        assert len(data) == 0

        # Create a project
        res = await client.post(
            f"{PREFIX}/", json=dict(name="name", project_dir="project dir")
        )
        p = res.json()

        # Verify that the project was created
        res = await client.get(f"{PREFIX}/")
        data = res.json()
        debug(data)
        assert res.status_code == 200
        assert len(data) == 1

        # Verify that the project has a dataset
        stm = select(Dataset).join(Project).where(Project.id == p["id"])
        res = await db.execute(stm)
        res = list(res)
        debug(res)
        assert len(res) == 1

        # Delete the project
        res = await client.delete(f"{PREFIX}/{p['id']}")
        assert res.status_code == 204

        # Check that the project was deleted
        res = await client.get(f"{PREFIX}/")
        data = res.json()
        assert len(data) == 0

        # Check that project-related datasets were deleted
        res = await db.execute(stm)
        res = list(res)
        debug(res)
        assert len(res) == 0


async def test_edit_resource(
    client, MockCurrentUser, project_factory, dataset_factory, resource_factory
):
    async with MockCurrentUser(persist=True) as user:
        prj = await project_factory(user)
        ds = await dataset_factory(project=prj)
        orig_resource = await resource_factory(dataset=ds)

        payload = dict(path="my/new/path")
        res = await client.patch(
            f"{PREFIX}/{prj.id}/{ds.id}/{orig_resource.id}", json=payload
        )
        data = res.json()
        debug(data)
        assert res.status_code == 200
        for key, value in payload.items():
            assert data[key] == value

        for key, value in orig_resource.dict().items():
            if key not in payload:
                assert data[key] == value


async def test_delete_dataset(
    client, MockCurrentUser, project_factory, dataset_factory, db
):
    async with MockCurrentUser(persist=True) as user:
        prj = await project_factory(user)
        ds0 = await dataset_factory(project=prj)
        ds1 = await dataset_factory(project=prj)

        ds_ids = (ds0.id, ds1.id)

        res = await client.get(f"{PREFIX}/{prj.id}")
        prj_dict = res.json()
        assert len(prj_dict["dataset_list"]) == 2
        assert prj_dict["dataset_list"][0]["id"] in ds_ids
        assert prj_dict["dataset_list"][1]["id"] in ds_ids

        # Add a resource to verify that the cascade works
        payload = dict(path="/some/absolute/path", glob_pattern="*.png")
        res = await client.post(
            f"{PREFIX}/{prj.id}/{ds0.id}",
            json=payload,
        )

        # Verify that the dataset contains a resource
        stm = select(Resource).join(Dataset).where(Dataset.id == ds0.id)
        res = await db.execute(stm)
        assert len([r for r in res]) == 1

        # Delete dataset
        res = await client.delete(f"{PREFIX}/{prj.id}/{ds0.id}")
        assert res.status_code == 204

        # Verify that the resources with the deleted
        # dataset id are deleted
        res = await db.execute(stm)
        assert len([r for r in res]) == 0

        res = await client.get(f"{PREFIX}/{prj.id}")
        prj_dict = res.json()
        assert len(prj_dict["dataset_list"]) == 1
        assert prj_dict["dataset_list"][0]["id"] == ds1.id


async def test_job_list(
    client,
    MockCurrentUser,
    project_factory,
    dataset_factory,
    workflow_factory,
    job_factory,
    tmp_path,
):
    async with MockCurrentUser(persist=True) as user:
        prj = await project_factory(user)

        # Test that the endpoint returns an empty job list
        res = await client.get(f"{PREFIX}/{prj.id}/jobs/")
        assert res.status_code == 200
        debug(res.json())
        assert len(res.json()) == 0

        # Create all needed objects in the database
        input_dataset = await dataset_factory(prj, name="input")
        output_dataset = await dataset_factory(prj, name="output")
        workflow = await workflow_factory(project_id=prj.id)
        job = await job_factory(
            project_id=prj.id,
            workflow_id=workflow.id,
            working_dir=(tmp_path / "some_working_dir").as_posix(),
            input_dataset_id=input_dataset.id,
            output_dataset_id=output_dataset.id,
        )
        debug(job)

        # Test that the endpoint returns a list with the new job
        res = await client.get(f"{PREFIX}/{prj.id}/jobs/")
        assert res.status_code == 200
        debug(res.json())
        assert len(res.json()) == 1
        assert res.json()[0]["project_id"] == prj.id
        assert res.json()[0]["id"] == job.id


async def test_job_download_logs(
    client,
    MockCurrentUser,
    project_factory,
    dataset_factory,
    workflow_factory,
    job_factory,
    tmp_path,
):
    async with MockCurrentUser(persist=True) as user:
        prj = await project_factory(user)

        # Create all needed objects in the database
        input_dataset = await dataset_factory(prj, name="input")
        output_dataset = await dataset_factory(prj, name="output")
        workflow = await workflow_factory(project_id=prj.id)
        working_dir = (tmp_path / "workflow_dir_for_zipping").as_posix()
        job = await job_factory(
            project_id=prj.id,
            workflow_id=workflow.id,
            working_dir=working_dir,
            input_dataset_id=input_dataset.id,
            output_dataset_id=output_dataset.id,
        )
        debug(job)

        # Write a log file in working_dir
        LOG_CONTENT = "This is a log\n"
        LOG_FILE = "log.txt"
        Path(working_dir).mkdir()
        with (Path(working_dir) / LOG_FILE).open("w") as f:
            f.write(LOG_CONTENT)

        # Test that the endpoint returns a list with the new job
        res = await client.get(f"/api/v1/job/download/{job.id}")
        assert res.status_code == 200
        assert (
            res.headers.get("content-type") == "application/x-zip-compressed"
        )

        # Write response into a zipped file
        zipped_archive_path = tmp_path / "logs.zip"
        debug(zipped_archive_path)
        with zipped_archive_path.open("wb") as f:
            f.write(res.content)

        # Unzip the log archive
        unzipped_archived_path = tmp_path / "unzipped_logs"
        debug(unzipped_archived_path)
        with ZipFile(zipped_archive_path, mode="r") as zipfile:
            zipfile.extractall(path=unzipped_archived_path)

        # Verify content of the unzipped log archive
        with (unzipped_archived_path / LOG_FILE).open("r") as f:
            actual_logs = f.read()
        assert LOG_CONTENT in actual_logs
