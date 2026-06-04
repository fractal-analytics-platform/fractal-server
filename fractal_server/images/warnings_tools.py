from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from fractal_server.app.models.v2 import HistoryImageCache
from fractal_server.app.models.v2 import HistoryUnit

IMAGE_HAS_WARNINGS_KEY = "has_warnings"


async def enrich_images_with_warning_flag(
    *,
    images: list[dict[str, Any]],
    dataset_id: int,
    workflowtask_id: int,
    db: AsyncSession,
) -> list[dict[str, Any]]:
    zarr_urls = [image["zarr_url"] for image in images]
    stm = (
        select(HistoryImageCache.zarr_url)
        .where(HistoryImageCache.zarr_url.in_(zarr_urls))
        .where(HistoryImageCache.dataset_id == dataset_id)
        .where(HistoryImageCache.workflowtask_id == workflowtask_id)
        .join(
            HistoryUnit,
            HistoryImageCache.latest_history_unit_id == HistoryUnit.id,
        )
        .where(HistoryUnit.has_warnings.is_(True))
    )
    res = await db.execute(stm)
    zarr_urls_with_warnings = res.scalars().all()
    return [
        {
            IMAGE_HAS_WARNINGS_KEY: (
                image["zarr_url"] in zarr_urls_with_warnings
            ),
            **image,
        }
        for image in images
    ]
