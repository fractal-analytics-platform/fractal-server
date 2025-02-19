from sqlmodel import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models.v2 import HistoryItemV2


def parse_history(
    *,
    dataset_id: int,
    workflowtask_id: int,
):
    """
    FIXME: this is the naive approach, which loops over *all*
    history items. We can likely do better. Examples:
    1. Do the concatenation in SQL (tried with func.jsonb_agg but failed).
    2. Do a reverse search, starting from the last record.
    """
    current_images = {}
    with next(get_sync_db()) as db:
        stm = (
            select(HistoryItemV2)
            .where(HistoryItemV2.dataset_id == dataset_id)
            .where(HistoryItemV2.workflowtask_id == workflowtask_id)
            .order_by(HistoryItemV2.timestamp_started)
        )
        history_items = db.execute(stm).scalars().all()
        for item in history_items:
            current_images.update(item.images)
    return current_images
