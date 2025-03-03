from fractal_server.app.history.status_enum import HistoryItemImageStatus
from fractal_server.app.models.v2 import HistoryItemV2
from fractal_server.app.models.v2 import ImageStatus


async def test_delete_workflow_associated_to_history(
    project_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    dataset_factory_v2,
    workflowtask_factory_v2,
    db,
    client,
    MockCurrentUser,
):
    async with MockCurrentUser() as user:

        task = await task_factory_v2(user_id=user.id)
        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(project_id=project.id)

        # Delete Workflow

        workflow = await workflow_factory_v2(project_id=project.id)
        wftask = await workflowtask_factory_v2(
            workflow_id=workflow.id, task_id=task.id
        )

        history = HistoryItemV2(
            dataset_id=dataset.id,
            workflowtask_id=wftask.id,
            workflowtask_dump={},
            task_group_dump={},
            parameters_hash="abc",
            num_available_images=0,
            num_current_images=0,
            images={},
        )
        db.add(history)
        await db.commit()

        await db.refresh(history)
        assert history.workflowtask_id == wftask.id

        await client.delete(
            f"/api/v2/project/{project.id}/workflow/{workflow.id}/"
        )

        await db.refresh(history)
        assert history.workflowtask_id is None

        # Delete WorkflowTask

        workflow = await workflow_factory_v2(project_id=project.id)
        wftask = await workflowtask_factory_v2(
            workflow_id=workflow.id, task_id=task.id
        )

        history = HistoryItemV2(
            dataset_id=dataset.id,
            workflowtask_id=wftask.id,
            workflowtask_dump={},
            task_group_dump={},
            parameters_hash="abc",
            num_available_images=0,
            num_current_images=0,
            images={},
        )
        db.add(history)
        await db.commit()

        await db.refresh(history)
        assert history.workflowtask_id == wftask.id

        await client.delete(
            "/api/v2/project/"
            f"{project.id}/workflow/{workflow.id}/wftask/{wftask.id}/"
        )

        await db.refresh(history)
        assert history.workflowtask_id is None


async def test_status(
    project_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    dataset_factory_v2,
    workflowtask_factory_v2,
    db,
    client,
    MockCurrentUser,
):
    async with MockCurrentUser() as user:

        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(project_id=project.id)
        workflow = await workflow_factory_v2(project_id=project.id)

        task = await task_factory_v2(user_id=user.id)
        wftask1 = await workflowtask_factory_v2(
            workflow_id=workflow.id, task_id=task.id
        )
        wftask2 = await workflowtask_factory_v2(
            workflow_id=workflow.id, task_id=task.id
        )
        wftask3 = await workflowtask_factory_v2(
            workflow_id=workflow.id, task_id=task.id
        )
        # WorkflowTask 1
        db.add(
            HistoryItemV2(
                workflowtask_id=wftask1.id,
                dataset_id=dataset.id,
                workflowtask_dump={},
                task_group_dump={},
                images={},
                parameters_hash="xxx",
                num_available_images=1,
                num_current_images=1,
            )
        )
        db.add(
            HistoryItemV2(
                images={},
                workflowtask_id=wftask1.id,
                dataset_id=dataset.id,
                workflowtask_dump={},
                task_group_dump={},
                parameters_hash="xxx",
                num_available_images=20,
                num_current_images=1,
            )
        )
        db.add(
            ImageStatus(
                zarr_url="/a",
                workflowtask_id=wftask1.id,
                dataset_id=dataset.id,
                parameters_hash="xxx",
                status=HistoryItemImageStatus.DONE,
                logfile="abc",
            )
        )
        db.add(
            ImageStatus(
                zarr_url="/b",
                workflowtask_id=wftask1.id,
                dataset_id=dataset.id,
                parameters_hash="xxx",
                status=HistoryItemImageStatus.DONE,
                logfile="abc",
            )
        )
        db.add(
            ImageStatus(
                zarr_url="/c",
                workflowtask_id=wftask1.id,
                dataset_id=dataset.id,
                parameters_hash="xxx",
                status=HistoryItemImageStatus.FAILED,
                logfile="abc",
            )
        )
        # WorkflowTask 2
        db.add(
            HistoryItemV2(
                images={},
                workflowtask_id=wftask2.id,
                dataset_id=dataset.id,
                workflowtask_dump={},
                task_group_dump={},
                parameters_hash="xxx",
                num_available_images=33,
                num_current_images=1,
            )
        )
        db.add(
            ImageStatus(
                zarr_url="/d",
                workflowtask_id=wftask2.id,
                dataset_id=dataset.id,
                parameters_hash="xxx",
                status=HistoryItemImageStatus.DONE,
                logfile="abc",
            )
        )
        db.add(
            ImageStatus(
                zarr_url="/e",
                workflowtask_id=wftask2.id,
                dataset_id=dataset.id,
                parameters_hash="xxx",
                status=HistoryItemImageStatus.SUBMITTED,
                logfile="abc",
            )
        )
        await db.commit()

        res = await client.get(
            f"/api/v2/project/{project.id}/status/"
            f"?workflow_id={workflow.id}&dataset_id={dataset.id}"
        )

        assert res.json() == {
            str(wftask1.id): {
                "num_done_images": 2,
                "num_submitted_images": 0,
                "num_failed_images": 1,
                "num_available_images": 20,
            },
            str(wftask2.id): {
                "num_done_images": 1,
                "num_submitted_images": 1,
                "num_failed_images": 0,
                "num_available_images": 33,
            },
            str(wftask3.id): None,
        }


async def test_status_subsets(
    project_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    dataset_factory_v2,
    workflowtask_factory_v2,
    db,
    client,
    MockCurrentUser,
):
    async with MockCurrentUser() as user:

        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(project_id=project.id)
        workflow = await workflow_factory_v2(project_id=project.id)

        task = await task_factory_v2(user_id=user.id)
        wftask = await workflowtask_factory_v2(
            workflow_id=workflow.id, task_id=task.id
        )
        # WorkflowTask 1
        db.add(
            HistoryItemV2(
                workflowtask_id=wftask.id,
                dataset_id=dataset.id,
                workflowtask_dump={"A": 1},
                task_group_dump={},
                images={},
                parameters_hash="xxx",
                num_available_images=1,
                num_current_images=1,
            )
        )
        db.add(
            ImageStatus(
                zarr_url="/a",
                workflowtask_id=wftask.id,
                dataset_id=dataset.id,
                parameters_hash="xxx",
                status=HistoryItemImageStatus.DONE,
                logfile="abc",
            )
        )
        db.add(
            ImageStatus(
                zarr_url="/b",
                workflowtask_id=wftask.id,
                dataset_id=dataset.id,
                parameters_hash="xxx",
                status=HistoryItemImageStatus.DONE,
                logfile="abc",
            )
        )
        db.add(
            HistoryItemV2(
                images={},
                workflowtask_id=wftask.id,
                dataset_id=dataset.id,
                workflowtask_dump={"B": 2},
                task_group_dump={},
                parameters_hash="yyy",
                num_available_images=20,
                num_current_images=1,
            )
        )
        db.add(
            ImageStatus(
                zarr_url="/c",
                workflowtask_id=wftask.id,
                dataset_id=dataset.id,
                parameters_hash="yyy",
                status=HistoryItemImageStatus.FAILED,
                logfile="abc",
            )
        )
        db.add(
            ImageStatus(
                zarr_url="/d",
                workflowtask_id=wftask.id,
                dataset_id=dataset.id,
                parameters_hash="yyy",
                status=HistoryItemImageStatus.SUBMITTED,
                logfile="abc",
            )
        )
        await db.commit()

        res = await client.get(
            f"/api/v2/project/{project.id}/status/subsets/"
            f"?workflowtask_id={wftask.id}&dataset_id={dataset.id}"
        )

        assert res.json() == [
            {
                "workflowtask_dump": {"A": 1},
                "parameters_hash": "xxx",
                "info": {
                    "num_done_images": 2,
                    "num_failed_images": 0,
                    "num_submitted_images": 0,
                },
            },
            {
                "workflowtask_dump": {"B": 2},
                "parameters_hash": "yyy",
                "info": {
                    "num_done_images": 0,
                    "num_failed_images": 1,
                    "num_submitted_images": 1,
                },
            },
        ]


async def test_status_images(
    project_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    dataset_factory_v2,
    workflowtask_factory_v2,
    db,
    client,
    MockCurrentUser,
):
    async with MockCurrentUser() as user:

        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(project_id=project.id)
        workflow = await workflow_factory_v2(project_id=project.id)

        task = await task_factory_v2(user_id=user.id)
        wftask1 = await workflowtask_factory_v2(
            workflow_id=workflow.id, task_id=task.id
        )

        # WorkflowTask 1
        for i in range(10):
            db.add(
                ImageStatus(
                    zarr_url=f"/image{i}",
                    workflowtask_id=wftask1.id,
                    dataset_id=dataset.id,
                    parameters_hash="xxx",
                    status=HistoryItemImageStatus.DONE,
                    logfile="abc",
                )
            )
        db.add(
            ImageStatus(
                zarr_url="/broken",
                workflowtask_id=wftask1.id,
                dataset_id=dataset.id,
                parameters_hash="xxx",
                status=HistoryItemImageStatus.FAILED,
                logfile="abc",
            )
        )
        db.add(
            ImageStatus(
                zarr_url="/new",
                workflowtask_id=wftask1.id,
                dataset_id=dataset.id,
                parameters_hash="yyy",
                status=HistoryItemImageStatus.DONE,
                logfile="abc",
            )
        )

        await db.commit()

        res = await client.get(
            f"/api/v2/project/{project.id}/status/images/"
            f"?workflowtask_id={wftask1.id}"
            f"&dataset_id={dataset.id}"
            "&status=done"
        )
        assert res.json()["total_count"] == 11

        res = await client.get(
            f"/api/v2/project/{project.id}/status/images/"
            f"?workflowtask_id={wftask1.id}"
            f"&dataset_id={dataset.id}"
            "&status=done"
            "&parameters_hash=xxx"
        )
        assert res.json()["total_count"] == 10

        res = await client.get(
            f"/api/v2/project/{project.id}/status/images/"
            f"?workflowtask_id={wftask1.id}"
            f"&dataset_id={dataset.id}"
            "&status=failed"
        )
        assert res.json()["total_count"] == 1

        res = await client.get(
            f"/api/v2/project/{project.id}/status/images/"
            f"?workflowtask_id={wftask1.id}"
            f"&dataset_id={dataset.id}"
            "&status=done"
            "&parameters_hash=xxx"
            "&page_size=3"
            "&page=2"
        )
        assert res.json() == {
            "total_count": 10,
            "page_size": 3,
            "current_page": 2,
            "images": ["/image3", "/image4", "/image5"],
        }

        res = await client.get(
            f"/api/v2/project/{project.id}/status/images/"
            f"?workflowtask_id={wftask1.id}"
            f"&dataset_id={dataset.id}"
            "&status=done"
            "&parameters_hash=xxx"
            "&page_size=3"
            "&page=4"
        )
        assert res.json() == {
            "total_count": 10,
            "page_size": 3,
            "current_page": 4,
            "images": ["/image9"],
        }

        res = await client.get(
            f"/api/v2/project/{project.id}/status/images/"
            f"?workflowtask_id={wftask1.id}"
            f"&dataset_id={dataset.id}"
            "&status=done"
            "&parameters_hash=xxx"
            "&page_size=3"
            "&page=999"
        )
        assert res.json() == {
            "total_count": 10,
            "page_size": 3,
            "current_page": 999,
            "images": [],
        }


async def test_cascade_delete_image_status(
    project_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    dataset_factory_v2,
    workflowtask_factory_v2,
    db,
    client,
    MockCurrentUser,
):
    async with MockCurrentUser() as user:

        task = await task_factory_v2(user_id=user.id)
        project = await project_factory_v2(user)

        dataset1 = await dataset_factory_v2(project_id=project.id)
        dataset2 = await dataset_factory_v2(project_id=project.id)

        workflow1 = await workflow_factory_v2(project_id=project.id)
        wftask1 = await workflowtask_factory_v2(
            workflow_id=workflow1.id, task_id=task.id
        )

        workflow2 = await workflow_factory_v2(project_id=project.id)
        wftask2a = await workflowtask_factory_v2(
            workflow_id=workflow2.id, task_id=task.id
        )
        wftask2b = await workflowtask_factory_v2(
            workflow_id=workflow2.id, task_id=task.id
        )

        image_status1 = ImageStatus(
            zarr_url="/a",
            workflowtask_id=wftask1.id,
            dataset_id=dataset1.id,
            parameters_hash="xxx",
            status=HistoryItemImageStatus.DONE,
            logfile="abc",
        )
        db.add(image_status1)
        key1 = ("/a", wftask1.id, dataset1.id)

        image_status2 = ImageStatus(
            zarr_url="/a",
            workflowtask_id=wftask1.id,
            dataset_id=dataset2.id,
            parameters_hash="xxx",
            status=HistoryItemImageStatus.DONE,
            logfile="abc",
        )
        db.add(image_status2)
        key2 = ("/a", wftask1.id, dataset2.id)

        image_status3a = ImageStatus(
            zarr_url="/a",
            workflowtask_id=wftask2a.id,
            dataset_id=dataset2.id,
            parameters_hash="xxx",
            status=HistoryItemImageStatus.DONE,
            logfile="abc",
        )
        db.add(image_status3a)
        key3a = ("/a", wftask2a.id, dataset2.id)
        image_status3b = ImageStatus(
            zarr_url="/a",
            workflowtask_id=wftask2b.id,
            dataset_id=dataset2.id,
            parameters_hash="xxx",
            status=HistoryItemImageStatus.DONE,
            logfile="abc",
        )
        db.add(image_status3b)
        key3b = ("/a", wftask2b.id, dataset2.id)

        await db.commit()
        db.expunge_all()

        # Delete `dataset1`
        res = await client.delete(
            f"/api/v2/project/{project.id}/dataset/{dataset1.id}/"
        )
        assert res.status_code == 204

        image_status = await db.get(ImageStatus, key1)
        assert image_status is None
        image_status = await db.get(ImageStatus, key2)
        assert image_status is not None
        image_status = await db.get(ImageStatus, key3a)
        assert image_status is not None
        image_status = await db.get(ImageStatus, key3b)
        assert image_status is not None

        # Delete `workflow1`
        res = await client.delete(
            f"/api/v2/project/{project.id}/workflow/{workflow1.id}/"
        )
        assert res.status_code == 204
        image_status = await db.get(ImageStatus, key2)
        assert image_status is None
        image_status = await db.get(ImageStatus, key3a)
        assert image_status is not None
        image_status = await db.get(ImageStatus, key3b)
        assert image_status is not None

        # Delete `wftask2a`
        res = await client.delete(
            f"/api/v2/project/{project.id}/workflow/{workflow2.id}/"
            f"wftask/{wftask2a.id}/"
        )
        assert res.status_code == 204
        image_status = await db.get(ImageStatus, key3a)
        assert image_status is None
        image_status = await db.get(ImageStatus, key3b)
        assert image_status is not None


async def test_get_image_logs(
    project_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    dataset_factory_v2,
    workflowtask_factory_v2,
    tmp_path,
    db,
    client,
    MockCurrentUser,
):
    async with MockCurrentUser() as user:

        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(project_id=project.id)
        workflow = await workflow_factory_v2(project_id=project.id)

        task = await task_factory_v2(user_id=user.id)
        wftask = await workflowtask_factory_v2(
            workflow_id=workflow.id, task_id=task.id
        )

        logfile = tmp_path / "logfile.log"
        with logfile.open("w") as f:
            f.write("Test log file")

        db.add(
            ImageStatus(
                zarr_url="/a",
                workflowtask_id=wftask.id,
                dataset_id=dataset.id,
                parameters_hash="xxx",
                status=HistoryItemImageStatus.DONE,
                logfile=logfile.as_posix(),
            )
        )
        db.add(
            ImageStatus(
                zarr_url="/b",
                workflowtask_id=wftask.id,
                dataset_id=dataset.id,
                parameters_hash="xxx",
                status=HistoryItemImageStatus.DONE,
                logfile=(tmp_path / "do_not_exists.log").as_posix(),
            )
        )
        db.add(
            ImageStatus(
                zarr_url="/c",
                workflowtask_id=wftask.id,
                dataset_id=dataset.id,
                parameters_hash="xxx",
                status=HistoryItemImageStatus.DONE,
                logfile=None,
            )
        )
        await db.commit()

        # Case 1: OK
        res = await client.post(
            f"/api/v2/project/{project.id}/status/image-logs/",
            json={
                "workflowtask_id": wftask.id,
                "dataset_id": dataset.id,
                "zarr_url": "/a",
            },
        )
        assert res.status_code == 200
        assert res.json() == "Test log file"

        # Case 2: wrong wftask
        res = await client.post(
            f"/api/v2/project/{project.id}/status/image-logs/",
            json={
                "workflowtask_id": wftask.id + 1,
                "dataset_id": dataset.id,
                "zarr_url": "/a",
            },
        )
        assert res.status_code == 404
        assert "WorkflowTask not found" in res.json()["detail"]

        # Case 3: logfile doesn't exist
        res = await client.post(
            f"/api/v2/project/{project.id}/status/image-logs/",
            json={
                "workflowtask_id": wftask.id,
                "dataset_id": dataset.id,
                "zarr_url": "/b",
            },
        )
        assert res.status_code == 200
        assert "Error while retrieving logs" in res.json()

        # Case 4: logfile is None
        res = await client.post(
            f"/api/v2/project/{project.id}/status/image-logs/",
            json={
                "workflowtask_id": wftask.id,
                "dataset_id": dataset.id,
                "zarr_url": "/c",
            },
        )
        assert res.status_code == 200
        assert "not yet available" in res.json()
