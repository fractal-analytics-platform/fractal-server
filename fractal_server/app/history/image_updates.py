from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified
from sqlmodel import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.history.status_enum import HistoryItemImageStatus
from fractal_server.app.models.v2 import HistoryItemV2
from fractal_server.app.models.v2 import ImageStatus


def _update_single_image_status(
    *,
    zarr_url: str,
    workflowtask_id: int,
    dataset_id: int,
    status: HistoryItemImageStatus,
    db: Session,
    commit: bool = True,
) -> None:
    image_status = db.get(
        ImageStatus,
        (
            zarr_url,
            workflowtask_id,
            dataset_id,
        ),
    )
    if image_status is None:
        raise RuntimeError("This should have not happened")
    image_status.status = status
    db.add(image_status)
    if commit:
        db.commit()


def update_single_image(
    *,
    history_item_id: int,
    zarr_url: str,
    status: HistoryItemImageStatus,
) -> None:
    # Note: thanks to `with_for_update`, a lock is acquired and kept
    # until `db.commit()`
    with next(get_sync_db()) as db:
        stm = (
            select(HistoryItemV2)
            .where(HistoryItemV2.id == history_item_id)
            .with_for_update(nowait=False)
        )
        history_item = db.execute(stm).scalar_one()
        history_item.images[zarr_url] = status
        flag_modified(history_item, "images")
        db.commit()

        _update_single_image_status(
            zarr_url=zarr_url,
            dataset_id=history_item.dataset_id,
            workflowtask_id=history_item.workflowtask_id,
            commit=True,
            status=status,
            db=db,
        )


def update_all_images(
    *,
    history_item_id: int,
    status: HistoryItemImageStatus,
) -> None:
    # Note: thanks to `with_for_update`, a lock is acquired and kept
    # until `db.commit()`
    stm = (
        select(HistoryItemV2)
        .where(HistoryItemV2.id == history_item_id)
        .with_for_update(nowait=False)
    )
    with next(get_sync_db()) as db:
        history_item = db.execute(stm).scalar_one()
        new_images = {
            zarr_url: status for zarr_url in history_item.images.keys()
        }
        history_item.images = new_images
        flag_modified(history_item, "images")
        db.commit()

        # FIXME: Make this a bulk edit, if possible
        for zarr_url in history_item.images.keys():
            _update_single_image_status(
                zarr_url=zarr_url,
                dataset_id=history_item.dataset_id,
                workflowtask_id=history_item.workflowtask_id,
                commit=False,
                status=status,
                db=db,
            )
        db.commit()
