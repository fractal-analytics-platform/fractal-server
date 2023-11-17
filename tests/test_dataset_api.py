from devtools import debug
from sqlmodel import select

from fractal_server.app.models import Dataset
from fractal_server.app.models import Resource
from fractal_server.app.schemas import JobStatusType

PREFIX = "api/v1"


async def test_get_dataset(
    app, client, MockCurrentUser, db, project_factory, dataset_factory
):
    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        dataset = await dataset_factory(project_id=project.id)
        project_id = project.id
        dataset_id = dataset.id
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


async def test_post_dataset(app, client, MockCurrentUser, db):
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


async def test_delete_dataset(
    client, MockCurrentUser, project_factory, dataset_factory, db
):
    async with MockCurrentUser(persist=True) as user:
        prj = await project_factory(user)
        ds0 = await dataset_factory(project_id=prj.id)
        ds1 = await dataset_factory(project_id=prj.id)

        ds_ids = (ds0.id, ds1.id)

        res = await client.get(f"{PREFIX}/project/{prj.id}")
        prj_dict = res.json()
        assert len(prj_dict["dataset_list"]) == 2
        assert prj_dict["dataset_list"][0]["id"] in ds_ids
        assert prj_dict["dataset_list"][1]["id"] in ds_ids

        # Add a resource to verify that the cascade works
        payload = dict(path="/some/absolute/path")
        res = await client.post(
            f"{PREFIX}/project/{prj.id}/dataset/{ds0.id}/resource/",
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


async def test_delete_dataset_failure(
    db,
    MockCurrentUser,
    project_factory,
    job_factory,
    tmp_path,
    workflow_factory,
    dataset_factory,
    task_factory,
    client,
):
    """
    GIVEN a Dataset in a relationship with an ApplyWorkflow
    WHEN we try to DELETE that Dataset via the correspondig endpoint
    THEN if the ApplyWorkflow is running the delete will fail,
         else the corresponding `dataset_id` is set None
    """
    async with MockCurrentUser(persist=True) as user:

        # Populate the database with the appropriate objects
        project = await project_factory(user)
        workflow = await workflow_factory(project_id=project.id)
        task = await task_factory(name="task", source="source")
        await workflow.insert_task(task_id=task.id, db=db)
        input_ds = await dataset_factory(project_id=project.id)
        output_ds = await dataset_factory(project_id=project.id)

        # Create a job in relationship with input_ds, output_ds and workflow
        job = await job_factory(
            project_id=project.id,
            workflow_id=workflow.id,
            input_dataset_id=input_ds.id,
            output_dataset_id=output_ds.id,
            working_dir=(tmp_path / "some_working_dir").as_posix(),
            status=JobStatusType.DONE,
        )
        assert job.input_dataset_id == input_ds.id
        assert job.output_dataset_id == output_ds.id

        # Assert that `Dataset.list_jobs_*` are correctly populated
        await db.refresh(input_ds)
        assert len(input_ds.list_jobs_input) == 1
        assert input_ds.list_jobs_input[0].id == job.id
        assert input_ds.list_jobs_output == []
        await db.refresh(output_ds)
        assert output_ds.list_jobs_input == []
        assert len(output_ds.list_jobs_output) == 1
        assert output_ds.list_jobs_output[0].id == job.id

        res = await client.delete(
            f"api/v1/project/{project.id}/dataset/{input_ds.id}"
        )
        assert res.status_code == 204

        await db.refresh(job)
        assert job.input_dataset_id is None
        assert job.output_dataset_id is not None

        res = await client.delete(
            f"api/v1/project/{project.id}/dataset/{output_ds.id}"
        )
        assert res.status_code == 204

        await db.refresh(job)
        assert job.input_dataset_id is None
        assert job.output_dataset_id is None

        # Assert tha we cannot stop a dataset linked to a running job
        ds_deletable_1 = await dataset_factory(id=1001, project_id=project.id)
        ds_deletable_2 = await dataset_factory(project_id=project.id)
        ds_not_deletable_1 = await dataset_factory(project_id=project.id)
        ds_not_deletable_2 = await dataset_factory(project_id=project.id)

        common_args = {
            "project_id": project.id,
            "workflow_id": workflow.id,
            "working_dir": (tmp_path / "some_working_dir").as_posix(),
        }
        await job_factory(
            input_dataset_id=ds_deletable_1.id,
            output_dataset_id=ds_not_deletable_1.id,
            status=JobStatusType.DONE,
            **common_args,
        )
        await job_factory(
            input_dataset_id=ds_not_deletable_2.id,
            output_dataset_id=ds_deletable_2.id,
            status=JobStatusType.FAILED,
            **common_args,
        )
        await job_factory(
            input_dataset_id=ds_not_deletable_1.id,
            output_dataset_id=ds_not_deletable_2.id,
            status=JobStatusType.SUBMITTED,  # this is why ds are not deletable
            **common_args,
        )
        await job_factory(
            input_dataset_id=ds_not_deletable_2.id,
            output_dataset_id=ds_not_deletable_1.id,
            status=JobStatusType.RUNNING,  # this is why ds are not deletable
            **common_args,
        )
        res = await client.delete(
            f"api/v1/project/{project.id}/dataset/{ds_deletable_1.id}"
        )
        assert res.status_code == 204
        res = await client.delete(
            f"api/v1/project/{project.id}/dataset/{ds_deletable_2.id}"
        )
        assert res.status_code == 204
        res = await client.delete(
            f"api/v1/project/{project.id}/dataset/{ds_not_deletable_1.id}"
        )
        assert res.status_code == 422
        res = await client.delete(
            f"api/v1/project/{project.id}/dataset/{ds_not_deletable_1.id}"
        )
        assert res.status_code == 422


async def test_patch_dataset(
    app, client, MockCurrentUser, db, project_factory, dataset_factory
):
    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        dataset = await dataset_factory(project_id=project.id)
        project_id = project.id
        dataset_id = dataset.id

        NEW_NAME = "something-new"
        res = await client.patch(
            f"{PREFIX}/project/{project_id}/dataset/{dataset_id}",
            json=dict(name=NEW_NAME),
        )
        assert res.status_code == 200

        res = await client.get(
            f"{PREFIX}/project/{project_id}/dataset/{dataset_id}",
        )
        assert res.status_code == 200
        dataset = res.json()
        debug(dataset)
        assert dataset["name"] == NEW_NAME

        # Check that history cannot be modified
        res = await client.patch(
            f"{PREFIX}/project/{project_id}/dataset/{dataset_id}",
            json=dict(history=[]),
        )
        assert res.status_code == 422


async def test_get_resource(
    db, client, MockCurrentUser, project_factory, dataset_factory
):
    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        dataset = await dataset_factory(project_id=project.id)
        res = await client.get(
            f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/resource/"
        )
        debug(res)
        assert res.status_code == 200


async def test_post_resource(
    db, client, MockCurrentUser, project_factory, dataset_factory
):
    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        project_id = project.id
        dataset = await dataset_factory(project_id=project.id, name="dataset")
        dataset_id = dataset.id

        # ADD RESOURCE TO DATASET / FAILURE
        payload = dict(path="non/absolute/path")
        res = await client.post(
            f"{PREFIX}/project/{project_id}/dataset/{dataset_id}/resource/",
            json=payload,
        )
        debug(res.json())
        assert res.status_code == 422
        resource = res.json()

        # ADD RESOURCE TO DATASET / SUCCESS
        payload = dict(path="/some/absolute/path")
        res = await client.post(
            f"{PREFIX}/project/{project_id}/dataset/{dataset_id}/resource/",
            json=payload,
        )
        assert res.status_code == 201
        resource = res.json()
        debug(resource)
        assert resource["path"] == payload["path"]


async def test_patch_resource(
    client, MockCurrentUser, project_factory, dataset_factory, resource_factory
):
    async with MockCurrentUser(persist=True) as user:
        prj = await project_factory(user)
        ds = await dataset_factory(project_id=prj.id)
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


async def test_patch_resource_failure(
    db, client, MockCurrentUser, project_factory, dataset_factory
):
    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        dataset = await dataset_factory(project_id=project.id)
        res = await client.post(
            f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/resource/",
            json=dict(path="/tmp/xyz"),
        )
        debug(res)
        assert res.status_code == 201
        resource = res.json()

        other_dataset = await dataset_factory(project_id=project.id)

        res = await client.patch(
            f"{PREFIX}/project/{project.id}/dataset/{other_dataset.id}/"
            f"resource/{resource['id']}",
            json=dict(path="/tmp/abc"),
        )
        assert res.status_code == 422
        assert res.json()["detail"] == (
            f"Resource {resource['id']} is not part "
            f"of dataset {other_dataset.id}"
        )


async def test_delete_resource(
    db, client, MockCurrentUser, project_factory, dataset_factory
):
    async with MockCurrentUser(persist=True) as user:
        # Create resource
        project = await project_factory(user)
        dataset = await dataset_factory(project_id=project.id)
        res = await client.post(
            f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/resource/",
            json=dict(path="/tmp/xyz"),
        )
        debug(res)
        assert res.status_code == 201
        resource = res.json()
        resource_id = resource["id"]

        # Failures due to invalid project, dataset or resource

        # Delete resource while using invalid project
        res = await client.delete(
            f"{PREFIX}/project/{987654321}/dataset/{dataset.id}/"
            f"resource/{resource_id}"
        )
        assert res.status_code == 404
        assert res.json()["detail"] == "Project not found"

        # Delete resource while using invalid dataset
        res = await client.delete(
            f"{PREFIX}/project/{project.id}/dataset/{987654321}/"
            f"resource/{resource_id}"
        )
        assert res.status_code == 404
        assert res.json()["detail"] == "Dataset not found"

        # Delete invalid resource
        res = await client.delete(
            f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/"
            f"resource/987654321"
        )
        assert res.status_code == 422
        assert res.json()["detail"] == (
            "Resource does not exist or does not belong to dataset"
        )

        # Successful deletion
        res = await client.delete(
            f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/"
            f"resource/{resource_id}"
        )
        assert res.status_code == 204

        # Create anohter project, dataset and resource
        other_project = await project_factory(user)
        other_dataset = await dataset_factory(project_id=other_project.id)
        res = await client.post(
            f"{PREFIX}/project/{other_project.id}/"
            f"dataset/{other_dataset.id}/resource/",
            json=dict(path="/tmp/xyz"),
        )
        assert res.status_code == 201
        other_resource = res.json()
        other_resource_id = other_resource["id"]

        # Delete resource while using wrong project/dataset
        res = await client.delete(
            f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/"
            f"resource/{other_resource_id}"
        )
        assert res.status_code == 422
        assert res.json()["detail"] == (
            "Resource does not exist or does not belong to dataset"
        )


async def test_post_resource_order(
    db,
    client,
    MockCurrentUser,
    project_factory,
    dataset_factory,
    resource_factory,
):
    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        project_id = project.id
        dataset = await dataset_factory(project_id=project.id, name="dataset")
        dataset_id = dataset.id

        resource_A = await resource_factory(dataset, id=2, path="/A")
        debug(resource_A)
        resource_B = await resource_factory(dataset, id=1, path="/B")
        debug(resource_B)

        res = await client.get(
            f"{PREFIX}/project/{project_id}/dataset/{dataset_id}",
        )
        assert res.status_code == 200
        dataset = res.json()
        debug(dataset)

        assert dataset["resource_list"][0]["id"] == 1
        assert dataset["resource_list"][0]["path"] == "/B"
        assert dataset["resource_list"][1]["id"] == 2
        assert dataset["resource_list"][1]["path"] == "/A"
