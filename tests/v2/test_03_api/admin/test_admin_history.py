from sqlmodel import select

from fractal_server.app.history.status_enum import HistoryItemImageStatus
from fractal_server.app.models.v2 import HistoryItemV2
from fractal_server.app.models.v2 import ImageStatus
from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)

PREFIX = "/admin/v2"


async def test_populate_image_status(
    db,
    client,
    MockCurrentUser,
    project_factory_v2,
    workflow_factory_v2,
    dataset_factory_v2,
    task_factory_v2,
):
    async with MockCurrentUser(user_kwargs={"is_superuser": False}) as user:

        project = await project_factory_v2(user)

        workflow = await workflow_factory_v2(project_id=project.id)

        task = await task_factory_v2(
            user_id=user.id, name="task", source="source"
        )
        dataset = await dataset_factory_v2(project_id=project.id)

        wftask = await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )

    db.add(
        HistoryItemV2(
            dataset_id=dataset.id,
            workflowtask_id=wftask.id,
            workflowtask_dump={},
            task_group_dump={},
            parameters_hash="xyz",
            num_available_images=42,
            num_current_images=42,
            images={
                "/a": HistoryItemImageStatus.DONE,
                "/b": HistoryItemImageStatus.DONE,
                "/c": HistoryItemImageStatus.FAILED,
            },
        )
    )
    db.add(
        HistoryItemV2(
            dataset_id=dataset.id,
            workflowtask_id=wftask.id,
            workflowtask_dump={},
            task_group_dump={},
            parameters_hash="abc",
            num_available_images=42,
            num_current_images=42,
            images={
                "/c": HistoryItemImageStatus.DONE,
            },
        )
    )
    db.add(
        ImageStatus(
            zarr_url="/xyz",
            workflowtask_id=wftask.id,
            dataset_id=dataset.id,
            parameters_hash="abc",
            status=HistoryItemImageStatus.DONE,
            logfile="/logfile.txt",
        )
    )
    await db.commit()

    async with MockCurrentUser(user_kwargs={"is_superuser": True}):

        res = await db.execute(select(ImageStatus))
        image_statuses = res.scalars().all()
        assert len(image_statuses) == 1

        res = await client.post(
            f"{PREFIX}/history/image-status/"
            f"?workflowtask_id={wftask.id}&dataset_id={dataset.id}"
        )
        assert res.status_code == 200
        assert len(res.json()) == 3

        res = await db.execute(select(ImageStatus))
        image_statuses = res.scalars().all()
        assert len(image_statuses) == 3
        assert "/xyz" not in [
            image_status.zarr_url for image_status in image_statuses
        ]
