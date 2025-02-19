import operator

from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from .status_enum import HistoryItemImageStatus
from fractal_server.app.db import get_sync_db
from fractal_server.app.models.v2 import HistoryItemV2


def _parse_image_status_list(
    images: dict[str, HistoryItemImageStatus],
) -> dict[str, int]:
    num_submitted_images = operator.countOf(
        images.values(),
        HistoryItemImageStatus.SUBMITTED,
    )
    num_done_images = operator.countOf(
        images.values(),
        HistoryItemImageStatus.DONE,
    )
    num_failed_images = operator.countOf(
        images.values(),
        HistoryItemImageStatus.FAILED,
    )
    result = dict(
        num_submitted_images=num_submitted_images,
        num_done_images=num_done_images,
        num_failed_images=num_failed_images,
    )
    return result


async def parse_history_given_async_db(
    *,
    dataset_id: int,
    workflowtask_ids: list[int],
    db: AsyncSession,
) -> dict[str, HistoryItemImageStatus]:
    output = {}
    for workflowtask_id in workflowtask_ids:
        stm = (
            select(HistoryItemV2.images)
            .where(HistoryItemV2.dataset_id == dataset_id)
            .where(HistoryItemV2.workflowtask_id == workflowtask_id)
            .order_by(HistoryItemV2.timestamp_started)
        )
        result = await db.execute(stm)
        history_items_images = result.scalars().all()
        current_images = {}
        for images in history_items_images:
            current_images.update(images)
        current_status = _parse_image_status_list(current_images)

        stm = (
            select(HistoryItemV2.num_available_images)
            .where(HistoryItemV2.dataset_id == dataset_id)
            .where(HistoryItemV2.workflowtask_id == workflowtask_id)
            .order_by(HistoryItemV2.timestamp_started.desc())
        )
        result = await db.execute(stm)
        num_available_images = result.scalars().first()
        current_status["num_available_images"] = num_available_images
        output[str(workflowtask_id)] = current_status

    return output


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
