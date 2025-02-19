from sqlalchemy.orm.attributes import flag_modified
from sqlmodel import select

from .runner import HistoryItemImageStatus
from fractal_server.app.db import get_sync_db
from fractal_server.app.models.v2 import HistoryItemV2


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


def update_all_images(
    *,
    history_item_id: int,
    status: HistoryItemImageStatus,
) -> None:
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
