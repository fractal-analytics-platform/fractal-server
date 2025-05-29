import time
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import HistoryImageCache
from fractal_server.images.status_tools import _prepare_query
from fractal_server.images.status_tools import enrich_images_sync


REPETITIONS = 4


def get_zarr_urls(db: Session, dataset_id: int, wftask_id: int):
    res = db.execute(
        select(
            HistoryImageCache.zarr_url,
        )
        .where(HistoryImageCache.dataset_id == dataset_id)
        .where(HistoryImageCache.workflowtask_id == wftask_id)
    )
    zarr_urls = res.scalars().all()
    size = len(zarr_urls)
    return zarr_urls[size // 4 : size // 2]


def create_fake_images_from_urls(zarr_urls: list[str]) -> list[dict]:
    zarr_urls_unset = [f"{zarr_url}-unset" for zarr_url in zarr_urls]
    return [
        {
            "zarr_url": zarr_url,
            "attributes": {},
            "types": {},
            "origin": None,
        }
        for zarr_url in (zarr_urls + zarr_urls_unset)
    ]


def measure_query_time(
    dataset_id: int,
    wftask_id: int,
    zarr_urls: list[int],
    db: Session,
) -> float:
    start = time.perf_counter()
    for rep in range(REPETITIONS):
        stm = _prepare_query(
            dataset_id=dataset_id,
            workflowtask_id=wftask_id,
            zarr_urls=zarr_urls,
        )
        db.execute(stm)
    end = time.perf_counter()
    avg_elapsed = (end - start) / REPETITIONS
    return avg_elapsed


def measure_enrich_image_time(
    images: list[dict[str, Any]],
    dataset_id: int,
    wftask_id: int,
) -> float:
    start = time.perf_counter()
    for rep in range(REPETITIONS):
        _ = enrich_images_sync(
            images=images,
            dataset_id=dataset_id,
            workflowtask_id=wftask_id,
        )
    end = time.perf_counter()
    avg_elapsed = (end - start) / REPETITIONS
    return avg_elapsed


if __name__ == "__main__":
    with next(get_sync_db()) as db:
        DATASET_ID = 2
        WORKFLOWTASK_ID = 2
        zarr_urls = get_zarr_urls(
            db=db,
            dataset_id=DATASET_ID,
            wftask_id=WORKFLOWTASK_ID,
        )
        images = create_fake_images_from_urls(zarr_urls=zarr_urls)
        query_time = measure_query_time(
            dataset_id=DATASET_ID,
            wftask_id=WORKFLOWTASK_ID,
            zarr_urls=zarr_urls,
            db=db,
        )
        enrich_time = measure_enrich_image_time(
            images=images,
            dataset_id=DATASET_ID,
            wftask_id=WORKFLOWTASK_ID,
        )

    print(f"{query_time=:.6f}, {enrich_time=:.6f}")
