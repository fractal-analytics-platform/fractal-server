from devtools import debug  # noqa

from fractal_server.app.history import HistoryItemImageStatus
from fractal_server.app.history import parse_history
from fractal_server.app.models.v2 import HistoryItemV2


async def test_update_image_status(
    db,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    workflowtask_factory_v2,
    task_factory_v2,
    MockCurrentUser,
):
    # Create test data
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user=user)
        datasetA = await dataset_factory_v2(project_id=project.id)
        datasetB = await dataset_factory_v2(project_id=project.id)
        workflow = await workflow_factory_v2(project_id=project.id)
        task = await task_factory_v2(user_id=user.id)
        wftask0 = await workflowtask_factory_v2(
            workflow_id=workflow.id,
            task_id=task.id,
        )
        wftask1 = await workflowtask_factory_v2(
            workflow_id=workflow.id,
            task_id=task.id,
        )

    common_dummy_args = dict(
        worfklowtask_dump={},
        task_group_dump={},
        parameters_hash="N/A",
        num_current_images=1,
        num_available_images=1,
    )
    # Actual history items to be considered
    db.add(
        HistoryItemV2(
            dataset_id=datasetA.id,
            workflowtask_id=wftask0.id,
            **common_dummy_args,
            images={
                "/1": HistoryItemImageStatus.FAILED,
                "/2": HistoryItemImageStatus.DONE,
                "/3": HistoryItemImageStatus.DONE,
            },
        )
    )
    db.add(
        HistoryItemV2(
            dataset_id=datasetA.id,
            workflowtask_id=wftask0.id,
            **common_dummy_args,
            images={
                "/1": HistoryItemImageStatus.FAILED,
            },
        )
    )

    # Spurious history items, corresponding to different
    # dataset or wftask
    db.add(
        HistoryItemV2(
            dataset_id=datasetA.id,
            workflowtask_id=wftask1.id,
            **common_dummy_args,
            images={},
        )
    )
    db.add(
        HistoryItemV2(
            dataset_id=datasetB.id,
            workflowtask_id=wftask0.id,
            **common_dummy_args,
            images={},
        )
    )
    await db.commit()
    db.expunge_all()

    parse_history(
        dataset_id=datasetA.id,
        workflowtask_id=wftask0.id,
    )
