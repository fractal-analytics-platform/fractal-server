from datetime import datetime
from typing import Any

from fastapi import Depends
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models.v2 import HistoryImageCache
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.schemas.v2 import HistoryUnitStatusQuery
from fractal_server.logger import set_logger


logger = set_logger(__name__)


# FIXME Add logger with start/end time
async def image_list_status_task(
    dataset_id: int,
    workflowtask_id: int,
    prefiltered_dataset_images: list[dict[str, Any]],
    # page_size: Optional[int],
    db: AsyncSession = Depends(get_async_db),
) -> list:
    start_time = datetime.now()
    logger.info(
        f"START {image_list_status_task.__name__} for {dataset_id=}, "
        f"{workflowtask_id=}, start_time={start_time.isoformat()}"
    )
    filtered_dataset_images_url = list(
        img["zarr_url"] for img in prefiltered_dataset_images
    )
    image_attributes_map = {
        img["zarr_url"]: img for img in prefiltered_dataset_images
    }

    stm = (
        select(HistoryImageCache.zarr_url, HistoryUnit.status)
        .join(HistoryUnit)
        .where(HistoryImageCache.dataset_id == dataset_id)
        .where(HistoryImageCache.workflowtask_id == workflowtask_id)
        .where(HistoryImageCache.latest_history_unit_id == HistoryUnit.id)
        .where(HistoryImageCache.zarr_url.in_(filtered_dataset_images_url))
        .order_by(HistoryImageCache.zarr_url)
    )
    res = await db.execute(stm)
    list_processed_url_status = res.all()
    logger.info(
        f"POST db query, "
        f"elapsed_time={(datetime.now() - start_time).total_seconds():.2f} "
        "seconds"
    )

    list_processed_url = list(item[0] for item in list_processed_url_status)
    logger.info(
        f"POST list_processed_url, "
        f"elapsed_time={(datetime.now() - start_time).total_seconds():.2f} "
        "seconds"
    )

    unprocessed_urls = set(image_attributes_map.keys()) - set(
        list_processed_url
    )
    list_non_processed_url_status = [
        (url, HistoryUnitStatusQuery.UNSET) for url in unprocessed_urls
    ]
    logger.info(
        f"POST list_non_processed_url_status, "
        f"elapsed_time={(datetime.now() - start_time).total_seconds():.2f} "
        "seconds"
    )
    full_list_url_status = (
        list_processed_url_status + list_non_processed_url_status
    )

    images_list = [
        dict(
            zarr_url=item[0],
            origin=image_attributes_map[item[0]]["origin"],
            types=image_attributes_map[item[0]]["types"],
            attributes={
                **image_attributes_map[item[0]]["attributes"],
                "status": item[1],
            },
        )
        for item in full_list_url_status
    ]

    return images_list
