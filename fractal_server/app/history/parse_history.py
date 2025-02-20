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


async def get_workflow_statuses(
    *,
    dataset_id: int,
    workflowtask_ids: list[int],
    db: AsyncSession,
) -> dict[str, HistoryItemImageStatus]:
    output = {}
    for workflowtask_id in workflowtask_ids:
        key = str(workflowtask_id)

        # Query database for a specific (wftask,dataset) pairs
        stm = (
            select(HistoryItemV2)
            .where(HistoryItemV2.dataset_id == dataset_id)
            .where(HistoryItemV2.workflowtask_id == workflowtask_id)
            .order_by(HistoryItemV2.timestamp_started)
        )
        result = await db.execute(stm)
        list_results = list(result.scalars().all())

        # Set value to None if this pair (wftask,dataset) was never run
        if len(list_results) == 0:
            output[key] = None
            continue

        # Merge images with their statuses, and keep track of latest
        # `num_available_images`
        current_images = {}
        for history_item in list_results:
            current_images.update(history_item.images)
            latest_num_available_images = history_item.num_available_images

        # Create and assign status object
        current_status = _parse_image_status_list(current_images)
        current_status["num_available_images"] = latest_num_available_images
        output[key] = current_status

    return output


async def get_workflowtask_image_statuses(
    *,
    dataset_id: int,
    workflowtask_id: int,
    db: AsyncSession,
) -> dict[str, HistoryItemImageStatus]:
    output = {}
    key = str(workflowtask_id)

    # Query database for a specific (wftask,dataset) pairs
    stm = (
        select(HistoryItemV2)
        .where(HistoryItemV2.dataset_id == dataset_id)
        .where(HistoryItemV2.workflowtask_id == workflowtask_id)
        .order_by(HistoryItemV2.timestamp_started)
    )
    result = await db.execute(stm)
    list_results = list(result.scalars().all())

    # Set value to None if this pair (wftask,dataset) was never run
    if len(list_results) == 0:
        return None

    # Merge images with their statuses, and keep track of latest
    # `num_available_images`
    current_images = {}
    for history_item in list_results:
        current_images.update(history_item.images)

    # Naive group-by-status operation
    # FIXME: Can we improve this, without all those `append`s?
    output = {status.value: [] for status in HistoryItemImageStatus}
    for key, value in current_images.items():
        output[value].append(key)

    # This repeats the loop once per status, but with list comprehension
    # all_statuses = [status.value for status in HistoryItemImageStatus]
    # for status in all_statuses:
    #     output[status] = [key for key, value in current_images if
    # value == status]

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
