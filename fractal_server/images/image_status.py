from typing import Any

from fastapi import Depends
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models.v2 import HistoryImageCache
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.schemas.v2 import HistoryUnitStatusQuery
from fractal_server.images.models import SingleImage
from fractal_server.logger import set_logger


logger = set_logger(__name__)


async def image_list_status_task(
    dataset_id: int,
    workflowtask_id: int,
    filtered_dataset_images: list[dict[str, Any]],
    db: AsyncSession = Depends(get_async_db),
) -> list:
    filtered_dataset_images_url = list(
        img["zarr_url"] for img in filtered_dataset_images
    )
    base_stmt = (
        select(HistoryImageCache.zarr_url, HistoryUnit.status)
        .join(HistoryUnit)
        .where(HistoryImageCache.dataset_id == dataset_id)
        .where(HistoryImageCache.workflowtask_id == workflowtask_id)
        .where(HistoryImageCache.latest_history_unit_id == HistoryUnit.id)
        .where(HistoryImageCache.zarr_url.in_(filtered_dataset_images_url))
        .order_by(HistoryImageCache.zarr_url)
    )

    stmt = base_stmt.order_by(HistoryImageCache.zarr_url)
    res = await db.execute(stmt)
    list_processed_url_status = res.all()

    list_processed_url = list(item[0] for item in list_processed_url_status)

    list_non_processed_url_status = list(
        (url, HistoryUnitStatusQuery.UNSET)
        for url in filtered_dataset_images_url
        if url not in list_processed_url
    )

    full_list_url_status = (
        list_processed_url_status + list_non_processed_url_status
    )
    sorted_images_list = [
        SingleImage(zarr_url=item[0], attributes=dict(status=item[1]))
        for item in sorted(
            full_list_url_status,
            key=lambda url_status: url_status[0],
        )
    ]
    return sorted_images_list
