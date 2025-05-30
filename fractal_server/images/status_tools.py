import time
from typing import Any

from sqlalchemy import Select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models.v2 import HistoryImageCache
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.schemas.v2 import HistoryUnitStatusWithUnset
from fractal_server.logger import set_logger

logger = set_logger(__name__)


IMAGE_STATUS_KEY = "__wftask_dataset_image_status__"


def _enriched_image(
    *,
    img: dict[str, Any],
    status: str,
) -> dict[str, Any]:
    return img | {
        "attributes": (img["attributes"] | {IMAGE_STATUS_KEY: status})
    }


def _prepare_query(
    *,
    dataset_id: int,
    workflowtask_id: int,
) -> Select:
    """
    Note: the query does not include `.order_by`.
    """
    stm = (
        select(HistoryImageCache.zarr_url, HistoryUnit.status)
        .join(HistoryUnit)
        .where(HistoryImageCache.dataset_id == dataset_id)
        .where(HistoryImageCache.workflowtask_id == workflowtask_id)
        .where(HistoryImageCache.latest_history_unit_id == HistoryUnit.id)
    )
    return stm


def _postprocess_image_lists(
    target_images: list[dict[str, Any]],
    list_query_url_status: list[tuple[str, str]],
) -> list[dict[str, Any]]:
    """ """
    t_1 = time.perf_counter()

    # Select only processed images that are part of the target image set
    zarr_url_to_image = {img["zarr_url"]: img for img in target_images}
    target_zarr_urls = zarr_url_to_image.keys()
    list_processed_url_status = [
        url_status
        for url_status in list_query_url_status
        if url_status[0] in target_zarr_urls
    ]

    set_processed_urls = set(
        url_status[0] for url_status in list_processed_url_status
    )
    processed_images_with_status = [
        _enriched_image(
            img=zarr_url_to_image[item[0]],
            status=item[1],
        )
        for item in list_processed_url_status
    ]

    non_processed_urls = target_zarr_urls - set_processed_urls
    non_processed_images_with_status = [
        _enriched_image(
            img=zarr_url_to_image[zarr_url],
            status=HistoryUnitStatusWithUnset.UNSET,
        )
        for zarr_url in non_processed_urls
    ]
    t_2 = time.perf_counter()
    logger.debug(
        f"[enrich_images_async] post-processing, elapsed={t_2 - t_1:.5f} s"
    )

    return processed_images_with_status + non_processed_images_with_status


async def enrich_images_unsorted_async(
    *,
    images: list[dict[str, Any]],
    dataset_id: int,
    workflowtask_id: int,
    db: AsyncSession,
) -> list[dict[str, Any]]:
    """
    Enrich images with a status-related attribute.

    Args:
        images: The input image list
        dataset_id: The dataset ID
        workflowtask_id: The workflow-task ID
        db: An async db session

    Returns:
        The list of enriched images, not necessarily in the same order as
        the input.
    """
    t_0 = time.perf_counter()
    logger.info(
        f"[enrich_images_async] START, {dataset_id=}, {workflowtask_id=}"
    )

    # Get `(zarr_url, status)` for _all_ processed images (including those that
    # are not part of the target image set)
    res = await db.execute(
        _prepare_query(
            dataset_id=dataset_id,
            workflowtask_id=workflowtask_id,
        )
    )
    list_query_url_status = res.all()
    t_1 = time.perf_counter()
    logger.debug(f"[enrich_images_async] query, elapsed={t_1 - t_0:.5f} s")

    output = _postprocess_image_lists(
        target_images=images,
        list_query_url_status=list_query_url_status,
    )

    return output


def enrich_images_unsorted_sync(
    *,
    images: list[dict[str, Any]],
    dataset_id: int,
    workflowtask_id: int,
) -> list[dict[str, Any]]:
    """
    Enrich images with a status-related attribute.


    Args:
        images: The input image list
        dataset_id: The dataset ID
        workflowtask_id: The workflow-task ID

    Returns:
        The list of enriched images, not necessarily in the same order as
        the input.
    """

    t_0 = time.perf_counter()
    logger.info(
        f"[enrich_images_async] START, {dataset_id=}, {workflowtask_id=}"
    )

    # Get `(zarr_url, status)` for _all_ processed images (including those that
    # are not part of the target image set)
    with next(get_sync_db()) as db:
        res = db.execute(
            _prepare_query(
                dataset_id=dataset_id,
                workflowtask_id=workflowtask_id,
            )
        )
        list_query_url_status = res.all()
    t_1 = time.perf_counter()
    logger.debug(f"[enrich_images_async] query, elapsed={t_1 - t_0:.5f} s")

    output = _postprocess_image_lists(
        target_images=images,
        list_query_url_status=list_query_url_status,
    )

    return output
