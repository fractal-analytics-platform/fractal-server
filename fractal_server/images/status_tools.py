import time
from copy import deepcopy
from typing import Any

from sqlalchemy import Select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models.v2 import HistoryImageCache
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.schemas.v2 import HistoryUnitStatusWithUnset
from fractal_server.logger import set_logger
from fractal_server.types import ImageAttributeValue

logger = set_logger(__name__)


IMAGE_STATUS_KEY = "__wftask_dataset_image_status__"


def _enriched_image(*, img: dict[str, Any], status: str) -> dict[str, Any]:
    img["attributes"][IMAGE_STATUS_KEY] = status
    return img


def _prepare_query(
    *,
    dataset_id: int,
    workflowtask_id: int,
    zarr_urls: list[str],
) -> Select:
    stm = (
        select(HistoryImageCache.zarr_url, HistoryUnit.status)
        .join(HistoryUnit)
        .where(HistoryImageCache.dataset_id == dataset_id)
        .where(HistoryImageCache.workflowtask_id == workflowtask_id)
        .where(HistoryImageCache.latest_history_unit_id == HistoryUnit.id)
        .where(HistoryImageCache.zarr_url.in_(zarr_urls))
        .order_by(HistoryImageCache.zarr_url)
    )
    return stm


async def enrich_images_async(
    *,
    images: list[dict[str, Any]],
    dataset_id: int,
    workflowtask_id: int,
    db: AsyncSession,
) -> list[dict[str, ImageAttributeValue]]:
    """
    Enrich images with a status-related attribute.

    Args:
        images: The input image list
        dataset_id: The dataset ID
        workflowtask_id: The workflow-task ID
        db: An async db session

    Returns:
        The list of enriched images
    """
    t_0 = time.perf_counter()
    logger.info(
        f"[enrich_images_async] START, {dataset_id=}, {workflowtask_id=}"
    )

    zarr_url_to_image = {img["zarr_url"]: deepcopy(img) for img in images}

    res = await db.execute(
        _prepare_query(
            dataset_id=dataset_id,
            workflowtask_id=workflowtask_id,
            zarr_urls=zarr_url_to_image.keys(),
        )
    )
    list_processed_url_status = res.all()
    t_1 = time.perf_counter()
    logger.debug(f"[enrich_images_async] db-query, elapsed={t_1 - t_0:.3f} s")

    set_processed_urls = set(item[0] for item in list_processed_url_status)
    processed_images_with_status = [
        _enriched_image(
            img=zarr_url_to_image[item[0]],
            status=item[1],
        )
        for item in list_processed_url_status
    ]
    t_2 = time.perf_counter()
    logger.debug(
        "[enrich_images_async] processed-images, " f"elapsed={t_2 - t_1:.3f} s"
    )

    non_processed_urls = zarr_url_to_image.keys() - set_processed_urls
    non_processed_images_with_status = [
        _enriched_image(
            img=zarr_url_to_image[zarr_url],
            status=HistoryUnitStatusWithUnset.UNSET,
        )
        for zarr_url in non_processed_urls
    ]
    t_3 = time.perf_counter()
    logger.debug(
        "[enrich_images_async] non-processed-images, "
        f"elapsed={t_3 - t_2:.3f} s"
    )

    return processed_images_with_status + non_processed_images_with_status


def enrich_images_sync(
    *,
    images: list[dict[str, Any]],
    dataset_id: int,
    workflowtask_id: int,
) -> list[dict[str, ImageAttributeValue]]:
    """
    Enrich images with a status-related attribute.

    Args:
        images: The input image list
        dataset_id: The dataset ID
        workflowtask_id: The workflow-task ID

    Returns:
        The list of enriched images
    """
    t_0 = time.perf_counter()
    logger.info(
        f"[enrich_images_async] START, {dataset_id=}, {workflowtask_id=}"
    )

    zarr_url_to_image = {img["zarr_url"]: deepcopy(img) for img in images}
    with next(get_sync_db()) as db:
        res = db.execute(
            _prepare_query(
                dataset_id=dataset_id,
                workflowtask_id=workflowtask_id,
                zarr_urls=zarr_url_to_image.keys(),
            )
        )
    list_processed_url_status = res.all()
    t_1 = time.perf_counter()
    logger.debug(f"[enrich_images_async] db-query, elapsed={t_1 - t_0:.3f} s")

    set_processed_urls = set(item[0] for item in list_processed_url_status)
    processed_images_with_status = [
        _enriched_image(
            img=zarr_url_to_image[item[0]],
            status=item[1],
        )
        for item in list_processed_url_status
    ]
    t_2 = time.perf_counter()
    logger.debug(
        "[enrich_images_async] processed-images, " f"elapsed={t_2 - t_1:.3f} s"
    )

    non_processed_urls = zarr_url_to_image.keys() - set_processed_urls
    non_processed_images_with_status = [
        _enriched_image(
            img=zarr_url_to_image[zarr_url],
            status=HistoryUnitStatusWithUnset.UNSET,
        )
        for zarr_url in non_processed_urls
    ]
    t_3 = time.perf_counter()
    logger.debug(
        "[enrich_images_async] non-processed-images, "
        f"elapsed={t_3 - t_2:.3f} s"
    )

    return processed_images_with_status + non_processed_images_with_status
