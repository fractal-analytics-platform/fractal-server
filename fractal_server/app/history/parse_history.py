from sqlmodel import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models.v2 import HistoryItemV2


def parse_history(
    *,
    dataset_id: int,
    workflowtask_id: int,
):
    with next(get_sync_db()) as db:
        stm = (
            select(HistoryItemV2)
            .where(HistoryItemV2.dataset_id == dataset_id)
            .where(HistoryItemV2.workflowtask_id == workflowtask_id)
            .order_by(HistoryItemV2.timestamp_started)
        )
        history_items = db.execute(stm).scalars().all()
        for item in history_items:
            print(item)
