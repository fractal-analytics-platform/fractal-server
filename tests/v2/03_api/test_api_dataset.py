from datetime import datetime
from datetime import timezone

from devtools import debug

from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.schemas.v2 import JobStatusTypeV2

PREFIX = "api/v2"


async def test_new_dataset_v2(client, MockCurrentUser):

    async with MockCurrentUser():

        res = await client.post("api/v2/project/", json=dict(name="projectV2"))
        debug(res.json())
        assert res.status_code == 201
        projectV2 = res.json()
        p2_id = projectV2["id"]

        # POST

        res = await client.post(
            f"api/v2/project/{p2_id}/dataset/",
            json=dict(name="dataset", zarr_dir="/tmp"),
        )
        assert res.status_code == 201
        dataset1 = res.json()

        res = await client.post(
            f"api/v2/project/{p2_id}/dataset/",
            json=dict(
                name="dataset",
                images=[
                    {
                        "path": "/tmp/xxx.yz",
                        "attributes": {"x": 10},
                    },
                    {
                        "path": "/tmp/xxx_corr.yz",
                        "attributes": {"x": 10, "y": True, "z": 3.14},
                    },
                ],
                filters={"x": 10},
                zarr_dir="/tmp",
            ),
        )
        assert res.status_code == 201
        dataset2 = res.json()

        # GET (3 different ones)

        # 1
        res = await client.get("api/v2/dataset/")
        assert res.status_code == 200
        user_dataset_list = res.json()

        # 2
        res = await client.get(f"api/v2/project/{p2_id}/dataset/")
        assert res.status_code == 200
        project_dataset_list = res.json()

        # 3
        res = await client.get(
            f"api/v2/project/{p2_id}/dataset/{dataset1['id']}/"
        )
        assert res.status_code == 200
        ds1 = res.json()
        res = await client.get(
            f"api/v2/project/{p2_id}/dataset/{dataset2['id']}/"
        )
        assert res.status_code == 200
        ds2 = res.json()

        assert user_dataset_list == project_dataset_list == [ds1, ds2]

        # UPDATE

        NEW_NAME = "new name"
        res = await client.patch(
            f"api/v2/project/{p2_id}/dataset/{dataset2['id']}/",
            json=dict(name=NEW_NAME),
        )
        assert res.status_code == 200
        res = await client.get(
            f"api/v2/project/{p2_id}/dataset/{dataset2['id']}/"
        )
        assert res.json()["name"] == NEW_NAME

        # DELETE

        res = await client.delete(
            f"api/v2/project/{p2_id}/dataset/{dataset2['id']}/"
        )
        assert res.status_code == 204
        res = await client.get(f"api/v2/project/{p2_id}/dataset/")
        assert len(res.json()) == 1


async def test_get_dataset(client, MockCurrentUser, project_factory_v2):
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        p_id = project.id
        # Create dataset
        DATASET_NAME = "My Dataset"
        res = await client.post(
            f"{PREFIX}/project/{p_id}/dataset/",
            json=dict(name=DATASET_NAME, zarr_dir="/tmp/zarr"),
        )
        assert res.status_code == 201
        ds_id = res.json()["id"]
        # Get project (useful to check dataset.project relationship)
        res = await client.get(f"{PREFIX}/project/{p_id}/")
        assert res.status_code == 200
        EXPECTED_PROJECT = res.json()
        # Get dataset, and check relationship
        res = await client.get(f"{PREFIX}/project/{p_id}/dataset/{ds_id}/")
        debug(res.json())
        assert res.status_code == 200
        assert (
            datetime.fromisoformat(res.json()["timestamp_created"]).tzinfo
            == timezone.utc
        )
        assert res.json()["name"] == DATASET_NAME
        assert res.json()["project"] == EXPECTED_PROJECT
        # Get missing dataset
        invalid_dataset_id = 999
        res = await client.get(
            f"{PREFIX}/project/{p_id}/dataset/{invalid_dataset_id}/",
        )
        assert res.status_code == 404

        # Get list of project datasets
        res = await client.get(f"{PREFIX}/project/{p_id}/dataset/")
        assert res.status_code == 200
        datasets = res.json()
        assert len(datasets) == 1
        assert datasets[0]["project"] == EXPECTED_PROJECT
        assert datasets[0]["history"] == []
        debug(datasets[0]["timestamp_created"])

        res = await client.get(
            f"{PREFIX}/project/{p_id}/dataset/?history=false"
        )
        assert res.status_code == 200
        datasets = res.json()
        assert len(datasets) == 1
        assert datasets[0]["project"] == EXPECTED_PROJECT
        assert datasets[0]["history"] == []
        debug(datasets[0]["timestamp_created"])


async def test_get_user_datasets(
    client, MockCurrentUser, project_factory_v2, dataset_factory_v2
):

    async with MockCurrentUser(user_kwargs={}) as user:

        project1 = await project_factory_v2(user, name="p1")
        project2 = await project_factory_v2(user, name="p2")
        await dataset_factory_v2(project_id=project1.id, name="ds1a")
        await dataset_factory_v2(project_id=project1.id, name="ds1b")
        await dataset_factory_v2(project_id=project2.id, name="ds2a")

        res = await client.get(f"{PREFIX}/dataset/")
        assert res.status_code == 200
        datasets = res.json()
        assert len(res.json()) == 3
        assert set(ds["name"] for ds in datasets) == {"ds1a", "ds1b", "ds2a"}
        for ds in datasets:
            assert len(ds["history"]) == 0

        res = await client.get(f"{PREFIX}/dataset/?history=false")
        assert res.status_code == 200
        datasets = res.json()
        assert len(res.json()) == 3
        assert set(ds["name"] for ds in datasets) == {"ds1a", "ds1b", "ds2a"}
        for ds in datasets:
            assert len(ds["history"]) == 0


async def test_post_dataset(client, MockCurrentUser):
    async with MockCurrentUser():
        # CREATE A PROJECT
        res = await client.post(
            f"{PREFIX}/project/",
            json=dict(name="test project"),
        )
        assert res.status_code == 201
        project = res.json()
        project_id = project["id"]

        # ADD DATASET
        payload = dict(name="new dataset", zarr_dir="/tmp/zarr")
        res = await client.post(
            f"{PREFIX}/project/{project_id}/dataset/",
            json=payload,
        )
        debug(res.json())
        assert res.status_code == 201
        dataset = res.json()
        assert dataset["name"] == payload["name"]
        assert dataset["project_id"] == project_id

        # EDIT DATASET
        payload1 = dict(name="new dataset name")
        res = await client.patch(
            f"{PREFIX}/project/{project_id}/dataset/{dataset['id']}/",
            json=payload1,
        )
        patched_dataset = res.json()
        debug(patched_dataset)
        assert res.status_code == 200
        for k, v in payload1.items():
            assert patched_dataset[k] == v
        assert patched_dataset["read_only"] == dataset["read_only"]


async def test_delete_dataset(
    client, MockCurrentUser, project_factory_v2, dataset_factory_v2
):
    async with MockCurrentUser() as user:
        prj = await project_factory_v2(user)
        ds0 = await dataset_factory_v2(project_id=prj.id)
        ds1 = await dataset_factory_v2(project_id=prj.id)

        ds_ids = (ds0.id, ds1.id)

        res = await client.get(f"{PREFIX}/project/{prj.id}/dataset/")
        datasets = res.json()
        assert len(datasets) == 2
        assert datasets[0]["id"] in ds_ids
        assert datasets[1]["id"] in ds_ids

        # Delete dataset
        res = await client.delete(
            f"{PREFIX}/project/{prj.id}/dataset/{ds0.id}/"
        )
        assert res.status_code == 204

        res = await client.get(f"{PREFIX}/project/{prj.id}/dataset/")
        datasets = res.json()
        assert len(datasets) == 1
        assert datasets[0]["id"] == ds1.id


async def test_delete_dataset_cascade_jobs(
    db,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    job_factory_v2,
    tmp_path,
    client,
):
    """
    GIVEN a Dataset in a relationship with an ApplyWorkflow
    WHEN we try to DELETE that Dataset via the correspondig endpoint
    THEN if the ApplyWorkflow is running the delete will fail,
         else the corresponding `dataset_id` is set None
    """
    async with MockCurrentUser() as user:

        # Populate the database with the appropriate objects
        project = await project_factory_v2(user)
        workflow = await workflow_factory_v2(project_id=project.id)
        task = await task_factory_v2(name="task", source="source")
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )
        dataset = await dataset_factory_v2(project_id=project.id)

        # Create a job in relationship with input_ds, output_ds and workflow
        job = await job_factory_v2(
            project_id=project.id,
            workflow_id=workflow.id,
            dataset_id=dataset.id,
            working_dir=(tmp_path / "some_working_dir").as_posix(),
            status=JobStatusTypeV2.DONE,
        )
        assert job.dataset_id == dataset.id

        res = await client.delete(
            f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/"
        )
        assert res.status_code == 204

        await db.refresh(job)
        assert job.dataset_id is None

        res = await client.delete(
            f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/"
        )
        assert res.status_code == 404

        # Assert tha we cannot stop a dataset linked to a running job
        ds_deletable = await dataset_factory_v2(id=2, project_id=project.id)
        ds_not_deletable = await dataset_factory_v2(
            id=3, project_id=project.id
        )

        common_args = {
            "project_id": project.id,
            "workflow_id": workflow.id,
            "working_dir": (tmp_path / "some_working_dir").as_posix(),
        }
        await job_factory_v2(
            dataset_id=ds_deletable.id,
            status=JobStatusTypeV2.DONE,
            **common_args,
        )
        await job_factory_v2(
            dataset_id=ds_deletable.id,
            status=JobStatusTypeV2.FAILED,
            **common_args,
        )
        await job_factory_v2(
            dataset_id=ds_not_deletable.id,
            status=JobStatusTypeV2.SUBMITTED,  # reason why ds is not deletable
            **common_args,
        )
        res = await client.delete(
            f"{PREFIX}/project/{project.id}/dataset/{ds_deletable.id}/"
        )
        assert res.status_code == 204
        res = await client.delete(
            f"{PREFIX}/project/{project.id}/dataset/{ds_not_deletable.id}/"
        )
        assert res.status_code == 422


async def test_patch_dataset(
    app, client, MockCurrentUser, project_factory_v2, dataset_factory_v2
):
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(project_id=project.id)
        project_id = project.id
        dataset_id = dataset.id

        NEW_NAME = "something-new"
        res = await client.patch(
            f"{PREFIX}/project/{project_id}/dataset/{dataset_id}/",
            json=dict(name=NEW_NAME),
        )
        assert res.status_code == 200

        res = await client.get(
            f"{PREFIX}/project/{project_id}/dataset/{dataset_id}/",
        )
        assert res.status_code == 200
        dataset = res.json()
        debug(dataset)
        assert dataset["name"] == NEW_NAME

        # Check that history cannot be modified
        res = await client.patch(
            f"{PREFIX}/project/{project_id}/dataset/{dataset_id}/",
            json=dict(history=[]),
        )
        debug(res.json())
        assert res.status_code == 422
