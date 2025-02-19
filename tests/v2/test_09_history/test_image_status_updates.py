import time
from concurrent.futures import ThreadPoolExecutor

from devtools import debug

from fractal_server.app.history import HistoryItemImageStatus
from fractal_server.app.history import update_all_images
from fractal_server.app.history import update_single_image
from fractal_server.app.models.v2 import HistoryItemV2


async def test_update_image_status(
    db,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    workflowtask_factory_v2,
    task_factory_v2,
    MockCurrentUser,
    monkeypatch,
):
    # Create test data
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user=user)
        dataset = await dataset_factory_v2(project_id=project.id)
        workflow = await workflow_factory_v2(project_id=project.id)
        task = await task_factory_v2(user_id=user.id)
        wftask = await workflowtask_factory_v2(
            workflow_id=workflow.id,
            task_id=task.id,
        )
    item = HistoryItemV2(
        dataset_id=dataset.id,
        workflowtask_id=wftask.id,
        worfklowtask_dump={},
        task_group_dump={},
        parameters_hash="xxx",
        num_current_images=1,
        num_available_images=1,
        images={
            "/url1": HistoryItemImageStatus.SUBMITTED,
            "/url2": HistoryItemImageStatus.SUBMITTED,
        },
    )
    db.add(item)
    await db.commit()
    await db.refresh(item)

    # Test single-image update
    update_single_image(
        history_item_id=item.id,
        zarr_url="/url1",
        status=HistoryItemImageStatus.DONE,
    )
    db.expunge_all()
    updated_item = await db.get(HistoryItemV2, item.id)
    debug(updated_item.images)
    assert updated_item.images == {
        "/url1": HistoryItemImageStatus.DONE,
        "/url2": HistoryItemImageStatus.SUBMITTED,
    }

    # Test all-images update
    update_all_images(
        history_item_id=item.id,
        status=HistoryItemImageStatus.FAILED,
    )
    db.expunge_all()
    updated_item = await db.get(HistoryItemV2, item.id)
    debug(updated_item.images)
    assert updated_item.images == {
        "/url1": HistoryItemImageStatus.FAILED,
        "/url2": HistoryItemImageStatus.FAILED,
    }

    # Test two concurrent single-image writes
    import fractal_server.app.history.image_updates
    from sqlalchemy.orm.attributes import flag_modified as raw_flag_modified

    def _flag_modified_patched(*args, **kwargs):
        time.sleep(1)
        return raw_flag_modified(*args, **kwargs)

    monkeypatch.setattr(
        fractal_server.app.history.image_updates,
        "flag_modified",
        _flag_modified_patched,
    )

    def wrap_update_single_image(zarr_url):
        update_single_image(
            history_item_id=item.id,
            zarr_url=zarr_url,
            status=HistoryItemImageStatus.DONE,
        )

    with ThreadPoolExecutor() as executor:
        executor.map(
            wrap_update_single_image,
            ["/url1", "/url2"],
        )

    db.expunge_all()
    updated_item = await db.get(HistoryItemV2, item.id)
    debug(updated_item.images)
    assert updated_item.images == {
        "/url1": HistoryItemImageStatus.DONE,
        "/url2": HistoryItemImageStatus.DONE,
    }
