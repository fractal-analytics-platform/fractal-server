from datetime import datetime
from datetime import timezone

from devtools import debug

from fractal_server.app.schemas.v1 import WorkflowTaskStatusTypeV1
from fractal_server.app.schemas.v1.dataset import _DatasetHistoryItemV1
from fractal_server.app.schemas.v1.dumps import TaskDumpV1
from fractal_server.app.schemas.v1.dumps import WorkflowTaskDumpV1

PREFIX = "api/v1"

HISTORY = [
    _DatasetHistoryItemV1(
        workflowtask=WorkflowTaskDumpV1(
            id=1,
            workflow_id=1,
            task_id=1,
            task=TaskDumpV1(
                id=1,
                source="...",
                name="test",
                command="echo",
                input_type="zarr",
                output_type="zarr",
            ).dict(),
        ).dict(),
        status=WorkflowTaskStatusTypeV1.DONE,
    ).dict()
]


async def test_get_dataset(
    app, client, MockCurrentUser, db, project_factory, dataset_factory
):
    async with MockCurrentUser() as user:
        project = await project_factory(user)
        p_id = project.id
        # Create dataset
        DATASET_NAME = "My Dataset"
        ds = await dataset_factory(
            name=DATASET_NAME, history=HISTORY, task_list=[]
        )
        ds_id = ds.id
        # Get project (useful to check dataset.project relationship)
        res = await client.get(f"/api/v1/project/{p_id}/")
        assert res.status_code == 200
        EXPECTED_PROJECT = res.json()
        # Get dataset, and check relationship
        res = await client.get(f"/api/v1/project/{p_id}/dataset/{ds_id}/")
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
        res = await client.get(f"/api/v1/project/{p_id}/dataset/")
        assert res.status_code == 200
        datasets = res.json()
        assert len(datasets) == 1
        assert datasets[0]["project"] == EXPECTED_PROJECT
        assert datasets[0]["history"] == HISTORY
        debug(datasets[0]["timestamp_created"])

        res = await client.get(
            f"/api/v1/project/{p_id}/dataset/?history=false"
        )
        assert res.status_code == 200
        datasets = res.json()
        assert len(datasets) == 1
        assert datasets[0]["project"] == EXPECTED_PROJECT
        assert datasets[0]["history"] == []
        debug(datasets[0]["timestamp_created"])


async def test_get_user_datasets(
    client, MockCurrentUser, project_factory, dataset_factory, db
):
    """
    Test /api/v1/dataset/
    """

    async with MockCurrentUser(user_kwargs={}) as user:
        debug(user)

        project1 = await project_factory(user, name="p1")
        project2 = await project_factory(user, name="p2")
        await dataset_factory(
            project_id=project1.id, name="ds1a", history=HISTORY
        )
        await dataset_factory(
            project_id=project1.id, name="ds1b", history=HISTORY
        )
        await dataset_factory(
            project_id=project2.id, name="ds2a", history=HISTORY
        )

        res = await client.get("/api/v1/dataset/")
        assert res.status_code == 200
        datasets = res.json()
        assert len(res.json()) == 3
        assert set(ds["name"] for ds in datasets) == {"ds1a", "ds1b", "ds2a"}
        for ds in datasets:
            assert len(ds["history"]) == 1

        res = await client.get("/api/v1/dataset/?history=false")
        assert res.status_code == 200
        datasets = res.json()
        assert len(res.json()) == 3
        assert set(ds["name"] for ds in datasets) == {"ds1a", "ds1b", "ds2a"}
        for ds in datasets:
            assert len(ds["history"]) == 0


async def test_get_resource(
    db, client, MockCurrentUser, project_factory, dataset_factory
):
    async with MockCurrentUser() as user:
        project = await project_factory(user)
        dataset = await dataset_factory(project_id=project.id)
        res = await client.get(
            f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/resource/"
        )
        debug(res)
        assert res.status_code == 200
