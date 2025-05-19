import time
from copy import deepcopy
from typing import Any

from fastapi import Depends
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models.v2 import HistoryImageCache
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.schemas.v2 import HistoryUnitStatusWithUnset
from fractal_server.logger import set_logger

IMAGE_STATUS_KEY = "__wftask_dataset_image_status__"

logger = set_logger(__name__)


def _enriched_image(*, img: dict[str, Any], status: str) -> dict[str, Any]:
    img["attributes"][IMAGE_STATUS_KEY] = status
    return img


async def enrich_image_list(
    *,
    images: list[dict[str, Any]],
    dataset_id: int,
    workflowtask_id: int,
    db: AsyncSession = Depends(get_async_db),
) -> list[dict[str, Any]]:
    start_time = time.perf_counter()
    logger.info(
        f"START {enrich_image_list.__name__} for {dataset_id=}, "
        f"{workflowtask_id=}"
    )

    zarr_url_to_image = {img["zarr_url"]: deepcopy(img) for img in images}

    stm = (
        select(HistoryImageCache.zarr_url, HistoryUnit.status)
        .join(HistoryUnit)
        .where(HistoryImageCache.dataset_id == dataset_id)
        .where(HistoryImageCache.workflowtask_id == workflowtask_id)
        .where(HistoryImageCache.latest_history_unit_id == HistoryUnit.id)
        .where(HistoryImageCache.zarr_url.in_(zarr_url_to_image.keys()))
        .order_by(HistoryImageCache.zarr_url)
    )
    res = await db.execute(stm)
    list_processed_url_status = res.all()
    logger.debug(
        f"POST db query, "
        f"elapsed={time.perf_counter() - start_time:.3f} "
        "seconds"
    )

    set_processed_urls = set(item[0] for item in list_processed_url_status)
    processed_images_with_status = [
        _enriched_image(
            img=zarr_url_to_image[item[0]],
            status=item[1],
        )
        for item in list_processed_url_status
    ]
    logger.debug(
        f"POST processed_images_with_status, "
        f"elapsed={time.perf_counter() - start_time:.3f} "
        "seconds"
    )

    non_processed_urls = zarr_url_to_image.keys() - set_processed_urls
    non_processed_images_with_status = [
        _enriched_image(
            img=zarr_url_to_image[zarr_url],
            status=HistoryUnitStatusWithUnset.UNSET,
        )
        for zarr_url in non_processed_urls
    ]
    logger.debug(
        f"POST non_processed_images_with_status, "
        f"elapsed={time.perf_counter() - start_time:.3f} "
        "seconds"
    )

    return processed_images_with_status + non_processed_images_with_status
