import json

from fractal_server.app.history.status_enum import HistoryItemImageStatus
from fractal_server.app.models.v2 import HistoryItemV2


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


async def test_history_details(
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

        # Define History items

        args = dict(
            dataset_id=dataset.id,
            task_group_dump={},
            num_available_images=0,
            num_current_images=0,
        )

        dump1 = wftask.model_dump()
        hash1 = json.dumps(dump1)
        db.add(
            HistoryItemV2(
                **args,
                workflowtask_id=wftask.id,
                workflowtask_dump=dump1,
                parameters_hash=hash1,
                images={
                    "a": HistoryItemImageStatus.DONE,
                    "b": HistoryItemImageStatus.DONE,
                    "f": HistoryItemImageStatus.FAILED,
                },
            )
        )
        db.add(
            HistoryItemV2(
                **args,
                workflowtask_id=wftask.id,
                workflowtask_dump=dump1,
                parameters_hash=hash1,
                images={
                    "c": HistoryItemImageStatus.FAILED,
                    "d": HistoryItemImageStatus.FAILED,
                },
            )
        )
        await db.commit()

        wftask.args_non_parallel = {"a": 1}
        db.add(wftask)
        await db.commit()

        dump2 = wftask.model_dump()
        hash2 = json.dumps(wftask.model_dump())
        db.add(
            HistoryItemV2(
                **args,
                workflowtask_id=wftask.id,
                workflowtask_dump=dump2,
                parameters_hash=hash2,
                images={
                    "c": HistoryItemImageStatus.FAILED,
                },
            )
        )
        await db.commit()

        wftask.args_non_parallel = {"a": 2}
        db.add(wftask)
        await db.commit()

        dump3 = wftask.model_dump()
        hash3 = json.dumps(wftask.model_dump())
        db.add(
            HistoryItemV2(
                **args,
                workflowtask_id=wftask.id,
                workflowtask_dump=dump3,
                parameters_hash=hash3,
                images={
                    "c": HistoryItemImageStatus.DONE,
                },
            )
        )
        db.add(
            HistoryItemV2(
                **args,
                workflowtask_id=wftask.id,
                workflowtask_dump=dump3,
                parameters_hash=hash3,
                images={
                    "d": HistoryItemImageStatus.DONE,
                },
            )
        )
        await db.commit()

        # Test endpoint

        res = await client.get(
            "/api/v2/history/details/"
            f"?workflowtask_id={wftask.id}&dataset_id={dataset.id}"
        )
        assert res.status_code == 200
        assert res.json() == [
            {
                "hash": hash1,
                "wftask_dump": dump1,
                "images": {
                    HistoryItemImageStatus.DONE: 2,
                    HistoryItemImageStatus.FAILED: 1,
                    HistoryItemImageStatus.SUBMITTED: 0,
                },
            },
            {
                "hash": hash3,
                "wftask_dump": dump3,
                "images": {
                    HistoryItemImageStatus.DONE: 2,
                    HistoryItemImageStatus.FAILED: 0,
                    HistoryItemImageStatus.SUBMITTED: 0,
                },
            },
        ]
