import pytest

# from fractal_server.app.history import ImageStatus
# from fractal_server.app.models.v2.history import HistoryItemV2
# from fractal_server.app.models.v2.history import ImageStatus


ZARR_URLS = ["a", "b", "c", "d"]


@pytest.fixture
async def mock_history_item(
    db,
    project_factory_v2,
    task_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    workflowtask_factory_v2,
    MockCurrentUser,
):
    # ) -> HistoryItemV2:

    # async with MockCurrentUser() as user:
    #     project = await project_factory_v2(user=user)
    #     task = await task_factory_v2(user_id=user.id)
    # ds = await dataset_factory_v2(project_id=project.id)
    # wf = await workflow_factory_v2(project_id=project.id)
    # wft = await workflowtask_factory_v2(
    #     workflow_id=wf.id,
    #     project_id=project.id,
    #     task_id=task.id,
    # )

    from pydantic import BaseModel

    class ObjectWithId(BaseModel):
        id: int

    return ObjectWithId(id=1)

    # parameters_hash = hash("something fake")
    # item = HistoryItemV2(
    #     dataset_id=ds.id,
    #     workflowtask_id=wft.id,
    #     workflowtask_dump={},
    #     task_group_dump={},
    #     parameters_hash=parameters_hash,
    #     num_current_images=4,
    #     num_available_images=4,
    #     images={
    #         zarr_url: UnitStatus.SUBMITTED
    #         for zarr_url in ZARR_URLS
    #     },
    # )
    # for zarr_url in ZARR_URLS:
    #     db.add(
    #         UnitStatus(
    #             zarr_url=zarr_url,
    #             workflowtask_id=wft.id,
    #             dataset_id=ds.id,
    #             parameters_hash=parameters_hash,
    #             status=UnitStatus.SUBMITTED,
    #             logfile="/invalid/placeholder",
    #         )
    #     )
    # db.add(item)
    # await db.commit()
    # await db.refresh(item)
    # return item
