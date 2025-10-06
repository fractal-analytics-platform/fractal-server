import sys
import time
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import HistoryImageCache
from fractal_server.images.status_tools import _prepare_query
from fractal_server.images.status_tools import enrich_images_unsorted_sync
from fractal_server.runner.v2.db_tools import bulk_upsert_image_cache_fast

REPETITIONS = 20
DATASET_ID = 2
WORKFLOWTASK_ID = 2
HISTORY_UNIT_ID = 2


def get_some_zarr_urls(db: Session, dataset_id: int, wftask_id: int):
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


def get_all_zarr_urls(db: Session, dataset_id: int, wftask_id: int):
    res = db.execute(
        select(
            HistoryImageCache.zarr_url,
        )
        .where(HistoryImageCache.dataset_id == dataset_id)
        .where(HistoryImageCache.workflowtask_id == wftask_id)
    )
    zarr_urls = res.scalars().all()
    return zarr_urls


def create_images() -> list[dict]:
    with next(get_sync_db()) as db:
        zarr_urls_processed = get_some_zarr_urls(
            db=db,
            dataset_id=DATASET_ID,
            wftask_id=WORKFLOWTASK_ID,
        )
    zarr_urls_unset = [f"{zarr_url}-unset" for zarr_url in zarr_urls_processed]
    return [
        {
            "zarr_url": zarr_url,
            "attributes": {},
            "types": {},
            "origin": None,
        }
        for zarr_url in (zarr_urls_processed + zarr_urls_unset)
    ]


def measure_query_time(
    dataset_id: int,
    wftask_id: int,
    zarr_urls: list[int],
) -> float:
    tot = 0.0
    for rep in range(REPETITIONS):
        with next(get_sync_db()) as db:
            start = time.perf_counter()
            stm = _prepare_query(
                dataset_id=dataset_id,
                workflowtask_id=wftask_id,
                # zarr_urls=zarr_urls,
            )
            res = db.execute(stm)
            res.all()
            end = time.perf_counter()
        tot += end - start
    avg_elapsed = tot / REPETITIONS
    return avg_elapsed


def measure_enrich_image_time(
    images: list[dict[str, Any]],
    dataset_id: int,
    wftask_id: int,
) -> float:
    start = time.perf_counter()
    for rep in range(REPETITIONS):
        enrich_images_unsorted_sync(
            images=images,
            dataset_id=dataset_id,
            workflowtask_id=wftask_id,
        )
    end = time.perf_counter()
    avg_elapsed = (end - start) / REPETITIONS
    return avg_elapsed


def measure_enrich_image_time_sorted(
    images: list[dict[str, Any]],
    dataset_id: int,
    wftask_id: int,
) -> float:
    start = time.perf_counter()
    for rep in range(REPETITIONS):
        output_images = enrich_images_unsorted_sync(
            images=images,
            dataset_id=dataset_id,
            workflowtask_id=wftask_id,
        )
        output_images = sorted(
            output_images,
            key=lambda img: img["zarr_url"],
        )
    end = time.perf_counter()
    avg_elapsed = (end - start) / REPETITIONS
    return avg_elapsed


def measure_bulk_upsert() -> float:
    with next(get_sync_db()) as db:
        zarr_urls = get_all_zarr_urls(
            db=db, dataset_id=DATASET_ID, wftask_id=WORKFLOWTASK_ID
        )
        list_upsert_objects = [
            dict(
                workflowtask_id=WORKFLOWTASK_ID,
                dataset_id=DATASET_ID,
                zarr_url=zarr_url,
                latest_history_unit_id=HISTORY_UNIT_ID,
            )
            for zarr_url in zarr_urls
        ]
        t_start = time.perf_counter()
        bulk_upsert_image_cache_fast(
            list_upsert_objects=list_upsert_objects,
            db=db,
        )
        elapsed = time.perf_counter() - t_start
        return elapsed


if __name__ == "__main__":
    num_clusters = int(sys.argv[1])
    num_units = int(sys.argv[2])

    images = create_images()
    zarr_urls = [img["zarr_url"] for img in images]

    query_time = measure_query_time(
        dataset_id=DATASET_ID,
        wftask_id=WORKFLOWTASK_ID,
        zarr_urls=zarr_urls,
    )

    enrich_time = measure_enrich_image_time(
        images=images,
        dataset_id=DATASET_ID,
        wftask_id=WORKFLOWTASK_ID,
    )

    enrich_time_with_sorted = measure_enrich_image_time_sorted(
        images=images,
        dataset_id=DATASET_ID,
        wftask_id=WORKFLOWTASK_ID,
    )
    upsert_time = measure_bulk_upsert()
    with open("out.csv", "a") as f:
        f.write(
            f"{num_clusters},{num_units},"
            f"{query_time:.7f},"
            f"{enrich_time:.7f},"
            f"{enrich_time_with_sorted:.7f},"
            f"{upsert_time:.7f}\n"
        )
