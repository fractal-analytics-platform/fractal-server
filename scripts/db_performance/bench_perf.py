import time
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import HistoryImageCache
from fractal_server.images.status_tools import _prepare_query
from fractal_server.images.status_tools import enrich_images_sync


def get_zarr_urls(db: Session, dataset_id: int, wftask_id: int):
    res = db.execute(
        select(
            HistoryImageCache.zarr_url,
        )
        .where(HistoryImageCache.dataset_id == dataset_id)
        .where(HistoryImageCache.workflowtask_id == wftask_id)
    )
    zarr_urls = [z_urls[0] for z_urls in res.all()]
    return zarr_urls


def measure_query_time(
    dataset_id: int, wftask_id: int, zarr_urls: list[int], db: Session
):
    stm = _prepare_query(
        dataset_id=dataset_id, workflowtask_id=wftask_id, zarr_urls=zarr_urls
    )
    start = time.perf_counter()
    res = db.execute(stm)
    url_status = res.all()
    end = time.perf_counter()
    print(f"Number of units: {len(url_status)} - elapsed: {end-start:.6f}")


def measure_enrich_image_time(
    images: list[dict[str, Any]], dataset_id: int, wftask_id: int
):
    start = time.perf_counter()
    _ = enrich_images_sync(
        images=images, dataset_id=dataset_id, workflowtask_id=wftask_id
    )
    end = time.perf_counter()
    print(end - start)


if __name__ == "__main__":
    with next(get_sync_db()) as db:
        DATASET_ID = 2
        WORKFLOWTASK_ID = 2
        zarr_urls = get_zarr_urls(
            db=db, dataset_id=DATASET_ID, wftask_id=WORKFLOWTASK_ID
        )
        measure_query_time(
            dataset_id=DATASET_ID,
            wftask_id=WORKFLOWTASK_ID,
            zarr_urls=zarr_urls,
            db=db,
        )
