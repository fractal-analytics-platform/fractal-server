from pathlib import Path
from zipfile import ZipFile

import pytest
from devtools import debug
from sqlmodel import select

from fractal_server.app.models import ApplyWorkflow
from fractal_server.app.models import Dataset
from fractal_server.app.models import Project
from fractal_server.app.models import Resource

PREFIX = "/api/v1"


async def test_project_get(client, db, project_factory, MockCurrentUser):

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


async def test_project_creation(app, client, MockCurrentUser, db):
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


async def test_project_creation_name_constraint(
    app, client, MockCurrentUser, db
):
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


@pytest.mark.parametrize("new_name", (None, "new name"))
@pytest.mark.parametrize("new_read_only", (None, True, False))
async def test_edit_project(
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


async def test_add_dataset(app, client, MockCurrentUser, db):

    async with MockCurrentUser(persist=True):

        # CREATE A PROJECT

        res = await client.post(
            f"{PREFIX}/project/",
            json=dict(name="test project"),
        )
        assert res.status_code == 201
        project = res.json()
        project_id = project["id"]

        # ADD DATASET

        payload = dict(
            name="new dataset",
            meta={"xy": 2},
        )
        res = await client.post(
            f"{PREFIX}/project/{project_id}/dataset/",
            json=payload,
        )
        assert res.status_code == 201
        dataset = res.json()
        assert dataset["name"] == payload["name"]
        assert dataset["project_id"] == project_id
        assert dataset["meta"] == payload["meta"]

        # EDIT DATASET

        payload1 = dict(name="new dataset name", meta={})
        res = await client.patch(
            f"{PREFIX}/project/{project_id}/dataset/{dataset['id']}",
            json=payload1,
        )
        patched_dataset = res.json()
        debug(patched_dataset)
        assert res.status_code == 200
        for k, v in payload1.items():
            assert patched_dataset[k] == payload1[k]
        assert patched_dataset["type"] == dataset["type"]
        assert patched_dataset["read_only"] == dataset["read_only"]

        payload2 = dict(type="new type", read_only=(not dataset["read_only"]))
        res = await client.patch(
            f"{PREFIX}/project/{project_id}/dataset/{dataset['id']}",
            json=payload2,
        )
        patched_dataset = res.json()
        debug(patched_dataset)
        assert res.status_code == 200
        for k, v in payload2.items():
            assert patched_dataset[k] == payload2[k]
        assert patched_dataset["name"] == payload1["name"]
        assert patched_dataset["meta"] == payload1["meta"]

        # ADD RESOURCE TO DATASET / FAILURE
        payload = dict(path="non/absolute/path")
        res = await client.post(
            f"{PREFIX}/project/{project_id}/dataset/{dataset['id']}/resource/",
            json=payload,
        )
        debug(res.json())
        assert res.status_code == 422
        resource = res.json()

        # ADD RESOURCE TO DATASET / SUCCESS
        payload = dict(path="/some/absolute/path")
        res = await client.post(
            f"{PREFIX}/project/{project_id}/dataset/{dataset['id']}/resource/",
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
            f"{PREFIX}/project/",
            json=dict(name="test project"),
        )
        assert res.status_code == 201
        project = res.json()
        debug(project)
        project_id = project["id"]
        dataset_id = project["dataset_list"][0]["id"]

        # Show existing dataset
        res = await client.get(
            f"{PREFIX}/project/{project_id}/dataset/{dataset_id}",
        )
        assert res.status_code == 200
        dataset = res.json()
        debug(dataset)
        assert dataset["project_id"] == project_id

        # Show missing dataset
        invalid_dataset_id = 999
        res = await client.get(
            f"{PREFIX}/project/{project_id}/dataset/{invalid_dataset_id}",
        )
        assert res.status_code == 404


async def test_add_dataset_local_path_error(app, client, MockCurrentUser, db):

    async with MockCurrentUser(persist=True):

        # CREATE A PROJECT

        res = await client.post(
            f"{PREFIX}/project/",
            json=dict(name="test project"),
        )
        assert res.status_code == 201
        project = res.json()
        project_id = project["id"]

        # ADD DATASET

        payload = dict(
            name="new dataset",
            meta={"xy": 2},
        )
        res = await client.post(
            f"{PREFIX}/project/{project_id}/dataset/",
            json=payload,
        )
        assert res.status_code == 201
        dataset = res.json()
        assert dataset["name"] == payload["name"]
        assert dataset["project_id"] == project_id
        assert dataset["meta"] == payload["meta"]

        # EDIT DATASET

        payload = dict(name="new dataset name", meta={})
        res = await client.patch(
            f"{PREFIX}/project/{project_id}/dataset/{dataset['id']}",
            json=payload,
        )
        patched_dataset = res.json()
        debug(patched_dataset)
        assert res.status_code == 200
        for k, v in payload.items():
            assert patched_dataset[k] == payload[k]

        # ADD WRONG RESOURCE TO DATASET

        payload = dict(path="some/local/path")
        debug(payload)

        res = await client.post(
            f"{PREFIX}/project/{project_id}/dataset/{dataset['id']}/resource/",
            json=payload,
        )
        assert res.status_code == 422


async def test_delete_project(
    client, MockCurrentUser, db, job_factory, workflow_factory, tmp_path
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

        # Check that a project-related dataset exists
        stm = select(Dataset).join(Project).where(Project.id == p["id"])
        res = await db.execute(stm)
        datasets = list(res)
        assert len(datasets) == 1
        dataset_id = datasets[0][0].id

        # Add a workflow to the project
        wf = await workflow_factory(project_id=p["id"])

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


async def test_edit_resource(
    client, MockCurrentUser, project_factory, dataset_factory, resource_factory
):
    async with MockCurrentUser(persist=True) as user:
        prj = await project_factory(user)
        ds = await dataset_factory(project=prj)
        orig_resource = await resource_factory(dataset=ds)

        payload = dict(path="/my/new/path")
        res = await client.patch(
            f"{PREFIX}/project/{prj.id}/dataset/{ds.id}/"
            f"resource/{orig_resource.id}",
            json=payload,
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

        res = await client.get(f"{PREFIX}/project/{prj.id}")
        prj_dict = res.json()
        assert len(prj_dict["dataset_list"]) == 2
        assert prj_dict["dataset_list"][0]["id"] in ds_ids
        assert prj_dict["dataset_list"][1]["id"] in ds_ids

        # Add a resource to verify that the cascade works
        payload = dict(path="/some/absolute/path")
        res = await client.post(
            f"{PREFIX}/project/{prj.id}/dataset/{ds0.id}",
            json=payload,
        )

        # Verify that the dataset contains a resource
        stm = select(Resource).join(Dataset).where(Dataset.id == ds0.id)
        res = await db.execute(stm)
        assert len([r for r in res]) == 1

        # Delete dataset
        res = await client.delete(
            f"{PREFIX}/project/{prj.id}/dataset/{ds0.id}"
        )
        assert res.status_code == 204

        # Verify that the resources with the deleted
        # dataset id are deleted
        res = await db.execute(stm)
        assert len([r for r in res]) == 0

        res = await client.get(f"{PREFIX}/project/{prj.id}")
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
        res = await client.get(f"{PREFIX}/project/{prj.id}/job/")
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
        res = await client.get(f"{PREFIX}/project/{prj.id}/job/")
        assert res.status_code == 200
        debug(res.json())
        assert len(res.json()) == 1
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
        res = await client.get(f"{PREFIX}/job/{job.id}/download/")
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


async def test_project_apply_failures(
    db,
    client,
    MockCurrentUser,
    project_factory,
    dataset_factory,
    workflow_factory,
    task_factory,
):
    async with MockCurrentUser(persist=True) as user:
        project1 = await project_factory(user)
        project2 = await project_factory(user)
        input_dataset = await dataset_factory(project1, name="input")
        output_dataset = await dataset_factory(project1, name="output")

        workflow1 = await workflow_factory(project_id=project1.id)
        workflow2 = await workflow_factory(project_id=project1.id)
        workflow3 = await workflow_factory(project_id=project2.id)

        task = await task_factory()
        await workflow1.insert_task(task.id, db=db)

        payload = dict(overwrite_input=False)
        # Not existing workflow
        res = await client.post(
            f"{PREFIX}/project/{project1.id}/workflow/123/apply/"
            f"?input_dataset_id={input_dataset.id}"
            f"&output_dataset_id={output_dataset.id}",
            json=payload,
        )
        debug(res.json())
        assert res.status_code == 404

        # Workflow with wrong project_id

        res = await client.post(
            f"{PREFIX}/project/{project1.id}/workflow/{workflow3.id}/apply/"
            f"?input_dataset_id={input_dataset.id}"
            f"&output_dataset_id={output_dataset.id}",
            json=payload,
        )
        debug(res.json())
        assert res.status_code == 422

        # Not existing output dataset
        res = await client.post(
            f"{PREFIX}/project/{project1.id}/workflow/{workflow1.id}/apply/"
            f"?input_dataset_id={input_dataset.id}&output_dataset_id=123",
            json=payload,
        )
        debug(res.json())
        assert res.status_code == 404

        # Failed auto_output_dataset
        res = await client.post(
            f"{PREFIX}/project/{project1.id}/workflow/{workflow1.id}/apply/"
            f"?input_dataset_id={input_dataset.id}",
            json=payload,
        )
        debug(res.json())
        assert res.status_code == 422

        # Workflow without tasks

        res = await client.post(
            f"{PREFIX}/project/{project1.id}/workflow/{workflow2.id}/apply/"
            f"?input_dataset_id={input_dataset.id}"
            f"&output_dataset_id={output_dataset.id}",
            json=payload,
        )
        debug(res.json())
        assert res.status_code == 422


async def test_project_apply_missing_user_attributes(
    db,
    client,
    MockCurrentUser,
    project_factory,
    dataset_factory,
    resource_factory,
    workflow_factory,
    task_factory,
    override_settings_factory,
):
    """
    When using the slurm backend, user.slurm_user and user.cache_dir become
    required attributes. If they are missing, the apply endpoint fails with a
    422 error.
    """

    override_settings_factory(FRACTAL_RUNNER_BACKEND="slurm")

    async with MockCurrentUser(persist=True) as user:

        # Make sure that user.cache_dir was not set
        debug(user)
        assert user.cache_dir is None

        # Create project, datasets, workflow, task, workflowtask
        project = await project_factory(user)
        input_dataset = await dataset_factory(
            project, name="input", type="zarr"
        )
        output_dataset = await dataset_factory(project, name="output")
        for dataset_id in [input_dataset.id, output_dataset.id]:
            res = await client.post(
                f"{PREFIX}/project/{project.id}/"
                f"dataset/{dataset_id}/resource/",
                json=dict(path="/some/absolute/path"),
            )
            assert res.status_code == 201
        workflow = await workflow_factory(project_id=project.id)
        task = await task_factory(input_type="zarr")
        await workflow.insert_task(task.id, db=db)

        # Call apply endpoint
        payload = dict(overwrite_input=False)
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={input_dataset.id}"
            f"&output_dataset_id={output_dataset.id}",
            json=payload,
        )
        debug(res.json())
        assert res.status_code == 422
        assert "user.cache_dir=None" in res.json()["detail"]

        user.cache_dir = "/tmp"
        user.slurm_user = None
        await db.merge(user)
        await db.commit()

        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={input_dataset.id}"
            f"&output_dataset_id={output_dataset.id}",
            json=payload,
        )
        debug(res.json())
        assert res.status_code == 422
        assert "user.slurm_user=None" in res.json()["detail"]


async def test_create_project(
    db,
    client,
    MockCurrentUser,
):
    async with MockCurrentUser(persist=True):
        # Payload without `name`
        empty_payload = {}
        res = await client.post(f"{PREFIX}/project/", json=empty_payload)
        debug(res.json())
        assert res.status_code == 422


async def test_no_resources(
    MockCurrentUser,
    project_factory,
    dataset_factory,
    workflow_factory,
    task_factory,
    db,
    client,
):
    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        input_dataset = await dataset_factory(
            project, name="input", type="zarr"
        )
        output_dataset = await dataset_factory(project, name="output")
        workflow = await workflow_factory(project_id=project.id)
        task = await task_factory()
        await workflow.insert_task(task.id, db=db)

        payload = dict(
            overwrite_input=False,
        )
        debug(input_dataset)
        debug(output_dataset)

        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={input_dataset.id}"
            f"&output_dataset_id={output_dataset.id}",
            json=payload,
        )

        debug(res.json())
        assert res.status_code == 422
        assert "empty resource_list" in res.json()["detail"]
