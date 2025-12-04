from devtools import debug
from sqlmodel import func

from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.schemas.v2 import JobStatusType
from fractal_server.app.schemas.v2.dataset import DatasetExport
from fractal_server.images import SingleImage
from fractal_server.string_tools import sanitize_string
from fractal_server.urls import normalize_url

PREFIX = "api/v2"


ZARR_DIR = "/zarr_dir"


def n_images(n: int) -> list[dict]:
    return [
        SingleImage(
            zarr_url=f"{ZARR_DIR}/{i}",
            attributes={
                str(i): i,
                "string_attribute": str(i % 2),
                "int_attribute": i % 2,
            },
            types={
                str(i): bool(i % 2),
                "flag": bool(i % 2 + 1),
            },
        ).model_dump()
        for i in range(n)
    ]


async def test_new_dataset(
    client,
    MockCurrentUser,
    local_resource_profile_db,
):
    resource, profile = local_resource_profile_db
    async with MockCurrentUser(user_kwargs=dict(profile_id=profile.id)) as user:
        res = await client.post("api/v2/project/", json=dict(name="project"))
        debug(res.json())
        assert res.status_code == 201
        project = res.json()
        p2_id = project["id"]

        # POST

        res = await client.post(
            f"api/v2/project/{p2_id}/dataset/",
            json=dict(
                name="dataset",
                project_dir=user.project_dirs[0],
                zarr_subfolder="tmp",
            ),
        )
        assert res.status_code == 201
        dataset1 = res.json()

        res = await client.post(
            f"api/v2/project/{p2_id}/dataset/",
            json=dict(
                name="dataset",
                project_dir=user.project_dirs[0],
                zarr_subfolder="tmp",
            ),
        )
        assert res.status_code == 201
        dataset2 = res.json()

        # GET (2 different ones)

        # 1
        res = await client.get(f"api/v2/project/{p2_id}/dataset/")
        assert res.status_code == 200
        project_dataset_list = res.json()

        # 2
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

        assert project_dataset_list == [ds1, ds2]

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


async def test_get_dataset(client, MockCurrentUser, project_factory):
    async with MockCurrentUser() as user:
        project = await project_factory(user)
        p_id = project.id
        # Create dataset
        DATASET_NAME = "My Dataset"
        res = await client.post(
            f"{PREFIX}/project/{p_id}/dataset/",
            json=dict(
                name=DATASET_NAME,
                project_dir=user.project_dirs[0],
                zarr_subfolder="tmp/zarr",
            ),
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
        debug(datasets[0]["timestamp_created"])


async def test_post_dataset(client, MockCurrentUser, project_factory, db):
    async with MockCurrentUser() as user:
        prj = await project_factory(user)

        # Check that zarr_dir must be relative to one of user's project dirs
        res = await db.execute(func.count(DatasetV2.id))
        initial_count = res.scalar()
        res = await client.post(
            f"{PREFIX}/project/{prj.id}/dataset/",
            json=dict(name="wrong", project_dir="/wrong"),
        )
        assert res.status_code == 403
        assert res.json()["detail"] == (
            "You are not allowed to use dataset.project_dir='/wrong'."
        )
        res = await db.execute(func.count(DatasetV2.id))
        assert res.scalar() == initial_count

        res = await client.post(
            f"{PREFIX}/project/{prj.id}/dataset/",
            json=dict(
                name="wrong",
                project_dir=user.project_dirs[0],
                zarr_subfolder="tmp/../zarr",
            ),
        )
        assert res.status_code == 422
        assert (
            "String must not contain '/../'."
            in (res.json()["detail"][0]["msg"])
        )

        # ADD DATASET
        payload = dict(
            name="new dataset",
            project_dir=user.project_dirs[0],
            zarr_subfolder="tmp/zarr",
        )
        res = await client.post(
            f"{PREFIX}/project/{prj.id}/dataset/",
            json=payload,
        )
        debug(res.json())
        assert res.status_code == 201
        dataset = res.json()
        assert dataset["name"] == payload["name"]
        assert dataset["project_id"] == prj.id

        # EDIT DATASET
        payload1 = dict(name="new dataset name")
        res = await client.patch(
            f"{PREFIX}/project/{prj.id}/dataset/{dataset['id']}/",
            json=payload1,
        )
        patched_dataset = res.json()
        debug(patched_dataset)
        assert res.status_code == 200
        for k, v in payload1.items():
            assert patched_dataset[k] == v

    # Test POST dataset without zarr_dir
    async with MockCurrentUser(
        user_kwargs={"project_dirs": ["/some/dir"]}
    ) as user:
        prj = await project_factory(user)
        res = await client.post(
            f"{PREFIX}/project/{prj.id}/dataset/", json=dict(name="DSName")
        )
        assert res.json()["zarr_dir"] == normalize_url(
            f"{user.project_dirs[0]}/fractal/"
            f"{prj.id}_{sanitize_string(prj.name)}/"
            f"{res.json()['id']}_{sanitize_string(res.json()['name'])}"
        )
        assert res.status_code == 201


async def test_delete_dataset(
    client, MockCurrentUser, project_factory, dataset_factory
):
    async with MockCurrentUser() as user:
        prj = await project_factory(user)
        ds0 = await dataset_factory(project_id=prj.id)
        ds1 = await dataset_factory(project_id=prj.id)

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
    project_factory,
    dataset_factory,
    workflow_factory,
    task_factory,
    job_factory,
    tmp_path,
    client,
):
    """
    GIVEN a Dataset in a relationship with an JobV2
    WHEN we try to DELETE that Dataset via the correspondig endpoint
    THEN if the JobV2 is running the delete will fail,
         else the corresponding `dataset_id` is set None
    """
    async with MockCurrentUser() as user:
        # Populate the database with the appropriate objects
        project = await project_factory(user)
        workflow = await workflow_factory(project_id=project.id)
        task = await task_factory(user_id=user.id, name="task")
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )
        dataset = await dataset_factory(project_id=project.id)

        # Create a job in relationship with dataset and workflow
        job = await job_factory(
            project_id=project.id,
            workflow_id=workflow.id,
            dataset_id=dataset.id,
            working_dir=(tmp_path / "some_working_dir").as_posix(),
            status=JobStatusType.DONE,
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
        ds_deletable = await dataset_factory(id=2, project_id=project.id)
        ds_not_deletable = await dataset_factory(id=3, project_id=project.id)

        common_args = {
            "project_id": project.id,
            "workflow_id": workflow.id,
            "working_dir": (tmp_path / "some_working_dir").as_posix(),
        }
        j1 = await job_factory(
            dataset_id=ds_deletable.id,
            status=JobStatusType.DONE,
            **common_args,
        )
        j2 = await job_factory(
            dataset_id=ds_deletable.id,
            status=JobStatusType.FAILED,
            **common_args,
        )
        await job_factory(
            dataset_id=ds_not_deletable.id,
            status=JobStatusType.SUBMITTED,  # reason why ds is not deletable
            **common_args,
        )
        res = await client.delete(
            f"{PREFIX}/project/{project.id}/dataset/{ds_deletable.id}/"
        )
        assert res.status_code == 204
        await db.refresh(j1)
        assert j1.dataset_id is None
        await db.refresh(j2)
        assert j2.dataset_id is None

        res = await client.delete(
            f"{PREFIX}/project/{project.id}/dataset/{ds_not_deletable.id}/"
        )
        assert res.status_code == 422


async def test_patch_dataset(
    app, client, MockCurrentUser, project_factory, dataset_factory
):
    async with MockCurrentUser() as user:
        project = await project_factory(user)
        dataset = await dataset_factory(
            project_id=project.id,
        )
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


async def test_dataset_import(
    client,
    MockCurrentUser,
    project_factory,
    db,
):
    PROJECT_DIR = "/user"
    ZARR_SUBFOLDER = "something"
    IMAGES = [
        SingleImage(
            zarr_url=f"{PROJECT_DIR}/{ZARR_SUBFOLDER}/image1"
        ).model_dump()
    ]
    EXPECTED_ATTRIBUTE_FILTERS = dict(key1=["value1"])

    async with MockCurrentUser(
        user_kwargs={"project_dirs": [PROJECT_DIR]}
    ) as user:
        project = await project_factory(user)
        ENDPOINT_URL = f"{PREFIX}/project/{project.id}/dataset/import/"

        # FAILURE: Images with zarr_urls not relative to zarr_dir
        payload = dict(
            name="Dataset",
            zarr_dir=f"/fake/{ZARR_SUBFOLDER}",
            images=IMAGES,
        )
        res = await client.post(ENDPOINT_URL, json=payload)
        debug(res.json())
        assert res.status_code == 422
        assert "is not relative to" in res.json()["detail"]

        # SUCCESS, with new filters (which are ignored)
        payload = dict(
            name="Dataset1",
            zarr_dir=f"{PROJECT_DIR}/{ZARR_SUBFOLDER}",
            images=IMAGES,
            attribute_filters=EXPECTED_ATTRIBUTE_FILTERS,
        )
        res = await client.post(ENDPOINT_URL, json=payload)
        assert res.status_code == 201
        res_dataset = res.json()
        debug(res_dataset)
        assert res_dataset["name"] == "Dataset1"
        assert res_dataset["zarr_dir"] == f"{PROJECT_DIR}/{ZARR_SUBFOLDER}"

        # SUCCESS, with legacy filters (which are ignored)
        payload = dict(
            name="Dataset2",
            zarr_dir=f"{PROJECT_DIR}/{ZARR_SUBFOLDER}",
            images=IMAGES,
            filters={
                "attributes": dict(key1="value1"),
                "types": dict(key3=True),
            },
        )
        res = await client.post(ENDPOINT_URL, json=payload)
        assert res.status_code == 201
        res_dataset = res.json()
        debug(res_dataset)
        assert res_dataset["name"] == "Dataset2"
        assert res_dataset["zarr_dir"] == f"{PROJECT_DIR}/{ZARR_SUBFOLDER}"

        # SUCCESS, with no filters
        payload = dict(
            name="Dataset3",
            zarr_dir=f"{PROJECT_DIR}/{ZARR_SUBFOLDER}",
            images=IMAGES,
        )
        res = await client.post(ENDPOINT_URL, json=payload)
        assert res.status_code == 201
        res_dataset = res.json()
        debug(res_dataset)
        assert res_dataset["name"] == "Dataset3"
        assert res_dataset["zarr_dir"] == f"{PROJECT_DIR}/{ZARR_SUBFOLDER}"

        # FAIL Cannot import dataset:
        # {image.zarr_url} is not relative to {dataset.zarr_dir}
        payload = dict(
            name="Dataset4",
            zarr_dir=f"{PROJECT_DIR}/{ZARR_SUBFOLDER}",
            images=[SingleImage(zarr_url="/something/image1").model_dump()],
        )
        res = await client.post(ENDPOINT_URL, json=payload)
        assert res.status_code == 422
        assert "Cannot import dataset" in res.json()["detail"]


async def test_export_dataset(
    client, MockCurrentUser, project_factory, dataset_factory
):
    async with MockCurrentUser() as user:
        project = await project_factory(user)
        dataset = await dataset_factory(project_id=project.id)
        res = await client.get(
            f"/api/v2/project/{project.id}/dataset/{dataset.id}/export/"
        )
        assert res.status_code == 200
        assert res.json() == DatasetExport(**dataset.model_dump()).model_dump()
