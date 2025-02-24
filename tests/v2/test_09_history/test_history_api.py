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
